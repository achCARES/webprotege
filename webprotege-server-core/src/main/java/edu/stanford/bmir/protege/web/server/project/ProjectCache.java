package edu.stanford.bmir.protege.web.server.project;

import com.google.auto.factory.AutoFactory;
import com.google.auto.factory.Provided;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import edu.stanford.bmir.protege.web.server.change.ChangeListGenerator;
import edu.stanford.bmir.protege.web.server.change.OntologyChange;
import edu.stanford.bmir.protege.web.server.change.RevisionAddedChangeListGenerator;
import edu.stanford.bmir.protege.web.server.change.RevisionReverterChangeListGenerator;
import edu.stanford.bmir.protege.web.server.change.RevisionReverterChangeListGeneratorFactory;
import edu.stanford.bmir.protege.web.server.diff.OntologyDiff2OntologyChanges;
import edu.stanford.bmir.protege.web.server.dispatch.impl.ProjectActionHandlerRegistry;
import edu.stanford.bmir.protege.web.server.events.EventManager;
import edu.stanford.bmir.protege.web.server.hierarchy.AnnotationPropertyHierarchyProviderImpl;
import edu.stanford.bmir.protege.web.server.hierarchy.ClassHierarchyProviderImpl;
import edu.stanford.bmir.protege.web.server.hierarchy.DataPropertyHierarchyProviderImpl;
import edu.stanford.bmir.protege.web.server.hierarchy.ObjectPropertyHierarchyProviderImpl;
import edu.stanford.bmir.protege.web.server.inject.ProjectComponent;
import edu.stanford.bmir.protege.web.server.merge.AnnotationDiffCalculator;
import edu.stanford.bmir.protege.web.server.merge.AxiomDiffCalculator;
import edu.stanford.bmir.protege.web.server.merge.ModifiedProjectOntologiesCalculator;
import edu.stanford.bmir.protege.web.server.merge.ModifiedProjectOntologiesCalculatorFactory;
import edu.stanford.bmir.protege.web.server.merge.OntologyDiffCalculator;
import edu.stanford.bmir.protege.web.server.owlapi.*;
import edu.stanford.bmir.protege.web.server.project.chg.ChangeManager;
import edu.stanford.bmir.protege.web.server.project.chg.ChangeManager_Factory;
import edu.stanford.bmir.protege.web.server.revision.Revision;
import edu.stanford.bmir.protege.web.server.revision.RevisionManager;
import edu.stanford.bmir.protege.web.server.revision.RevisionStore;
import edu.stanford.bmir.protege.web.server.revision.RevisionStoreFactory;
import edu.stanford.bmir.protege.web.shared.HasDispose;
import edu.stanford.bmir.protege.web.shared.csv.DocumentId;
import edu.stanford.bmir.protege.web.shared.event.ProjectEvent;
import edu.stanford.bmir.protege.web.shared.inject.ApplicationSingleton;
import edu.stanford.bmir.protege.web.shared.merge.OntologyDiff;
import edu.stanford.bmir.protege.web.shared.project.*;
import edu.stanford.bmir.protege.web.shared.revision.RevisionNumber;
import edu.stanford.bmir.protege.web.shared.user.UserId;

import java.text.Annotation;
import java.text.Normalizer;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf;
import org.semanticweb.owlapi.formats.NTriplesDocumentFormat;
import org.semanticweb.owlapi.io.OWLParserException;
import org.semanticweb.owlapi.io.StringDocumentSource;
import org.semanticweb.owlapi.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder.*;

/**
 * Author: Matthew Horridge<br>
 * Stanford University<br>
 * Bio-Medical Informatics Research Group<br>
 * Date: 07/03/2012
 */
@ApplicationSingleton
public class ProjectCache implements HasDispose {

    private static final String MSG_CHANGE_ENDPOINT = "Changed at endpoint.";

    private static final int ENDPOINT_CHECKING_INTERVAL_MINS = 1;

    private static final Logger logger = LoggerFactory.getLogger(ProjectCache.class);

    private final Interner<ProjectId> projectIdInterner;

    private final ReadWriteLock projectMapReadWriteLoc = new ReentrantReadWriteLock();

    private final Lock readLock = projectMapReadWriteLoc.readLock();

    private final Lock writeLock = projectMapReadWriteLoc.writeLock();

    private final Map<ProjectId, ProjectComponent> projectId2ProjectComponent = new ConcurrentHashMap<>();

    private final ReadWriteLock lastAccessLock = new ReentrantReadWriteLock();

    private final Map<ProjectId, Long> lastAccessMap = new HashMap<>();

    private final ProjectImporterFactory projectImporterFactory;

    @Nonnull
    private final ModifiedProjectOntologiesCalculatorFactory diffCalculatorFactory;

    /**
     * Elapsed time from the last access after which a project should be considered dormant (and should therefore
     * be purged).  This can interact with the frequency with which clients poll the project event queue (which is
     * be default every 10 seconds).
     */
    private final long dormantProjectTime;

    private final ProjectComponentFactory projectComponentFactory;

    @Nonnull
    private final ProjectDetailsManager projectDetailsManager;

    @Nonnull
    private final RevisionStoreFactory revisionStoreFactory;

    @Nonnull
    private final OntologyDiff2OntologyChanges ontologyDiff2OntologyChanges;

    @Inject
    public ProjectCache(@Nonnull ProjectComponentFactory projectComponentFactory,
                        @Nonnull ProjectImporterFactory projectImporterFactory,
                        @Nonnull ProjectDetailsManager projectDetailsManager,
                        @Nonnull ModifiedProjectOntologiesCalculatorFactory modifiedProjectOntologiesCalculatorFactory,
                        @Nonnull RevisionStoreFactory revisionStoreFactory,
                        @Nonnull OntologyDiff2OntologyChanges ontologyDiff2OntologyChanges,
                        @DormantProjectTime  long dormantProjectTime) {
        this.projectComponentFactory = checkNotNull(projectComponentFactory);
        this.projectImporterFactory = checkNotNull(projectImporterFactory);
        this.projectDetailsManager = projectDetailsManager;
        this.diffCalculatorFactory = modifiedProjectOntologiesCalculatorFactory;
        this.revisionStoreFactory = checkNotNull(revisionStoreFactory);
        this.ontologyDiff2OntologyChanges = checkNotNull(ontologyDiff2OntologyChanges);


        projectIdInterner = Interners.newWeakInterner();
        this.dormantProjectTime = dormantProjectTime;
        logger.info("Dormant project time: {} milliseconds", dormantProjectTime);
    }

    public ProjectActionHandlerRegistry getActionHandlerRegistry(ProjectId projectId) {
        return getProjectInternal(projectId, AccessMode.NORMAL, InstantiationMode.EAGER).getActionHandlerRegistry();
    }


    /**
     * Gets the list of cached project ids.
     * @return A list of cached project ids.
     */
    private List<ProjectId> getCachedProjectIds() {
        try {

            readLock.lock();
            return new ArrayList<>(lastAccessMap.keySet());
        }
        finally {
            readLock.unlock();
        }
    }

    /**
     * Purges projects that have not been access for some given period of time
     */
    public void purgeDormantProjects() {
        // No locking needed
        for (ProjectId projectId : getCachedProjectIds()) {
            long time = getLastAccessTime(projectId);
            long lastAccessTimeDiff = System.currentTimeMillis() - time;
            if (time == 0 || lastAccessTimeDiff > dormantProjectTime) {
                purge(projectId);
            }
        }
    }

    public void purgeAllProjects() {
        logger.info("Purging all loaded projects");
        for (ProjectId projectId : getCachedProjectIds()) {
            purge(projectId);
        }
    }

    public void ensureProjectIsLoaded(ProjectId projectId) throws ProjectDocumentNotFoundException {
        var projectComponent = getProjectInternal(projectId, AccessMode.NORMAL, InstantiationMode.EAGER);
        logger.info("Loaded {}", projectComponent.getProjectId());
    }

    public RevisionManager getRevisionManager(ProjectId projectId) {
        return getProjectInternal(projectId, AccessMode.NORMAL, InstantiationMode.LAZY).getRevisionManager();
    }

    @Nonnull
    public Optional<EventManager<ProjectEvent<?>>> getProjectEventManagerIfActive(@Nonnull ProjectId projectId) {
        try {
            readLock.lock();
            boolean active = isActive(projectId);
            if(!active) {
                return Optional.empty();
            }
            else {
                return Optional.of(getProjectInternal(projectId, AccessMode.QUIET, InstantiationMode.LAZY))
                        .map(ProjectComponent::getEventManager);
            }
        }
        finally {
            readLock.unlock();
        }
    }

    private enum AccessMode {
        NORMAL,
        QUIET
    }

    private ProjectComponent getProjectInternal(ProjectId projectId, AccessMode accessMode, InstantiationMode instantiationMode) {
        // Per project lock
        synchronized (getInternedProjectId(projectId)) {
            try {
                ProjectComponent projectComponent = getProjectInjector(projectId, instantiationMode);
                ensureCurrentOntology(projectComponent);

                if (accessMode == AccessMode.NORMAL) {
                    logProjectAccess(projectId);
                }
                return projectComponent;
            }
            catch (OWLParserException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void ensureCurrentOntology(ProjectComponent projectComponent) {
        ProjectId projectId = projectComponent.getProjectId();
        long lastAccessTime =  getLastAccessTime(projectId);
        Clock clock = Clock.systemUTC();
        long currentTime = Instant.now(clock).toEpochMilli();

        if (lastAccessTime < currentTime - ENDPOINT_CHECKING_INTERVAL_MINS * 60 * 1000) {
            if (projectDetailsManager.isExistingProject(projectId)) {
                RevisionManager revisionManager = projectComponent.getRevisionManager();
                RevisionNumber currentRevisionNo = revisionManager.getCurrentRevision();

                OWLOntologyManager owlOntologyManager = projectComponent.getRevisionManager()
                        .getOntologyManagerForRevision(currentRevisionNo);
                ProjectDetails projectDetails = projectDetailsManager.getProjectDetails(projectId);
                UserId userId = projectDetails.getOwner();
                Set<OWLOntology> projectOntologies = owlOntologyManager.getOntologies();
                IRI projectEndpoint = IRI.create(projectDetails.getProjectEndpoint());
                OWLLoader loader = new SparqlEndpointOWLLoaderFactory().setEndpoint(projectEndpoint)
                        .setTboxGraph(projectDetails.getTboxGraph()).get();
                OWLOntology endpointStoredOntology = loader.LoadOntology(projectEndpoint);

                for (OWLOntology ontology : projectOntologies) {
                    if (ontology.getOntologyID().equals(endpointStoredOntology.getOntologyID())) {
                        if (!ontology.getAxioms().equals(endpointStoredOntology.getAxioms())) {
                            ImmutableList<OntologyChange> changes = getOntologyChanges(ontology, endpointStoredOntology);
                            Revision rev = revisionManager.addRevision(userId, changes, MSG_CHANGE_ENDPOINT);
                            projectComponent.getChangeManager().applyChanges(userId,
                                    projectComponent.getRevisionAddedChangeListGeneratorFactory().create(rev.getRevisionNumber()));
                        }
                    }
                }
            }
            logProjectAccess(projectId);
        }
    }



    private ImmutableList<OntologyChange> getOntologyChanges(OWLOntology ontologyA, OWLOntology ontologyB) {
        HashSet<Ontology> ontologySetA = new HashSet<>();
        HashSet<Ontology> ontologySetB = new HashSet<>();
        ontologySetA.add(Ontology.get(ontologyA.getOntologyID(),
            ontologyA.getImportsDeclarations(),
            ontologyA.getAnnotations(),
            ontologyA.getAxioms()));
        ontologySetB.add(Ontology.get(ontologyB.getOntologyID(),
            ontologyB.getImportsDeclarations(),
            ontologyB.getAnnotations(),
            ontologyB.getAxioms()));
        ModifiedProjectOntologiesCalculator calculator = diffCalculatorFactory.create(
            ontologySetA, ontologySetB);
        Set<OntologyDiff> ontologyDiffSet = calculator.getModifiedOntologyDiffs();
        ImmutableList.Builder<OntologyChange> changeList = ImmutableList.builder();
        for(OntologyDiff diff : ontologyDiffSet) {
            List<OntologyChange> changes = ontologyDiff2OntologyChanges.getOntologyChangesFromDiff(diff);
            changeList.addAll(filterOutChangesDifferentOnlyByBlankNodeId(changes, ontologyB));
        }
        return changeList.build();
    }

    private List<OntologyChange> filterOutChangesDifferentOnlyByBlankNodeId(List<OntologyChange> changes, OWLOntology ontology) {
        Iterator<OntologyChange> it = changes.iterator();
        Set<OntologyChange> changesToRemove = new HashSet<>();
        Set<OWLAnnotation> annotationsAll = ontology.getAnnotations();
        Set<OWLAxiom> axiomsAll = ontology.getAxioms();

        while (it.hasNext()) {
            OntologyChange change = it.next();
            if (isChangeToRemove(change, annotationsAll, axiomsAll)) {
                changesToRemove.add(change);
            }
        }

        for (OntologyChange rmChange : changesToRemove) {
            changes.remove(rmChange);
        }

        return changes;
    }

    private boolean isChangeToRemove(OntologyChange change, Set<OWLAnnotation> annotationsAll, Set<OWLAxiom> axiomsAll) {
        if (change.isRemoveOntologyAnnotation() || change.isAddOntologyAnnotation()) {
            if (isAnnotationChangeToRemove(change, annotationsAll)) {
                return true;
            }
        } else if (change.isAxiomChange() || change.isRemoveAxiom() || change.isAddAxiom()) {
            if (isAxiomChangeToRemove(change, axiomsAll)) {
                return true;
            }
        }

        return false;
    }

    private boolean isAnnotationChangeToRemove(OntologyChange change, Set<OWLAnnotation> annotationsAll) {
        try {
            OWLAnnotation annotation = change.getAnnotationOrThrow();
            if (!annotation.getAnonymousIndividuals().isEmpty()) {
                HashSet<OWLAnnotation> sameAn = getEqualAnnotationsExceptForBlankNodes(annotation, annotationsAll);
                if (!sameAn.isEmpty()) {
                    return true;
                }
            }
        } catch (NoSuchElementException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private boolean isAxiomChangeToRemove(OntologyChange change, Set<OWLAxiom> axiomsAll) {
        try {
            OWLAxiom axiom = change.getAxiomOrThrow();
            if (!axiom.getAnonymousIndividuals().isEmpty()) {
                HashSet<OWLAxiom> sameAx = getEqualAxiomsExceptForBlankNodes(axiom, axiomsAll);
                if (!sameAx.isEmpty()) {
                    return true;
                }
            }
        } catch (NoSuchElementException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private String removeBlankNodeIds(String st) {
        return st.replaceAll("\\p{Zs}_:[a-zA-Z0-9]+", "")
                .replaceAll("_:[a-zA-Z0-9]+\\p{Zs}", "")
                .replaceAll("\\p{Zs}_:[a-zA-Z0-9]+\\p{Zs}", "");
    }

    private HashSet<OWLAnnotation> getEqualAnnotationsExceptForBlankNodes(OWLAnnotation annotation, Set<OWLAnnotation> annotationsAll) {
        HashSet<OWLAnnotation> sameAn = new HashSet<>();
        String annotationS = removeBlankNodeIds(annotation.toString());
        for (OWLAnnotation ontAnnotation : annotationsAll) {
            try {
                if (!ontAnnotation.getAnonymousIndividuals().isEmpty()) {
                    String ontAnnotationS = removeBlankNodeIds(annotation.toString());
                    if (annotationS.equals(ontAnnotationS)) {
                        sameAn.add(ontAnnotation);
                    }
                }
            } catch (NoSuchElementException e) {
                logger.error(e.getMessage());
            }
        }
        return sameAn;
    }

    private HashSet<OWLAxiom> getEqualAxiomsExceptForBlankNodes(OWLAxiom axiom, Set<OWLAxiom> axiomsAll) {
        HashSet<OWLAxiom> sameAx = new HashSet<>();
        String axiomS = removeBlankNodeIds(axiom.toString());
        for (OWLAxiom ontAxiom : axiomsAll) {
            try {
                if (!ontAxiom.getAnonymousIndividuals().isEmpty()) {
                    String ontAxiomS = removeBlankNodeIds(ontAxiom.toString());
                    if (axiomS.equals(ontAxiomS)) {
                        sameAx.add(ontAxiom);
                    }
                }
            } catch (NoSuchElementException e) {
                logger.error(e.getMessage());
            }
        }
        return sameAx;
    }
    
    private ProjectComponent getProjectInjector(ProjectId projectId, InstantiationMode instantiationMode) {
        ProjectComponent projectComponent = projectId2ProjectComponent.get(projectId);
        if (projectComponent == null) {
            logger.info("Request for unloaded project {}.", projectId.getId());
            Stopwatch stopwatch = Stopwatch.createStarted();
            projectComponent = projectComponentFactory.createProjectComponent(projectId);
            if(instantiationMode == InstantiationMode.EAGER) {
                // Force instantiation of certain objects in the project graph.
                // This needs to be done in a nicer way, but this approach works for now.
                projectComponent.init();
            }
            stopwatch.stop();
            logger.info("{} Instantiated project component in {} ms",
                        projectId,
                        stopwatch.elapsed(TimeUnit.MILLISECONDS));
            projectId2ProjectComponent.put(projectId, projectComponent);
        }
        return projectComponent;
    }

    /**
     * Gets an interned {@link ProjectId} that is equal to the specified {@link ProjectId}.
     * @param projectId The project id to intern.
     * @return The interned project Id.  Not {@code null}.
     */
    private ProjectId getInternedProjectId(ProjectId projectId) {
        // The interner is thread safe.
        return projectIdInterner.intern(projectId);
    }

    public ProjectId getProject(NewProjectSettings newProjectSettings) throws ProjectAlreadyExistsException, OWLOntologyCreationException, IOException {
        ProjectId projectId = ProjectIdFactory.getFreshProjectId();
        Optional<DocumentId> sourceDocumentId = newProjectSettings.getSourceDocumentId();
        IRI sparqlEndpoint = IRI.create(newProjectSettings.getProjectEndpoint());
        String tboxGraph = newProjectSettings.getTboxGraph();
        if(sourceDocumentId.isPresent()) {
            ProjectImporter importer = projectImporterFactory.create(projectId);
            importer.createProjectFromSources(sourceDocumentId.get(), newProjectSettings.getProjectOwner(),
                sparqlEndpoint, tboxGraph);
        }
        ProjectComponent projectComponent = getProjectInternal(projectId, AccessMode.NORMAL, InstantiationMode.EAGER);

        return projectComponent.getProjectId();
    }

    public void purge(ProjectId projectId) {
        try {
            writeLock.lock();
            lastAccessLock.writeLock().lock();
            var projectComponent = projectId2ProjectComponent.remove(projectId);
            if(projectComponent != null) {
                var projectDisposableObjectManager = projectComponent.getDisposablesManager();
                projectDisposableObjectManager.dispose();
            }
            lastAccessMap.remove(projectId);
        }
        finally {
            final int projectsBeingAccessed = lastAccessMap.size();
            lastAccessLock.writeLock().unlock();
            writeLock.unlock();
            logger.info("Purged project: {}.  {} projects are now being accessed.", projectId.getId(), projectsBeingAccessed);
        }
    }

    public boolean isActive(ProjectId projectId) {
        try {
            readLock.lock();
            return projectId2ProjectComponent.containsKey(projectId) && lastAccessMap.containsKey(projectId);
        }
        finally {
            readLock.unlock();
        }
    }

    /**
     * Gets the time of last cache access for a given project.
     * @param projectId The project id.
     * @return The time stamp of the last access of the specified project from the cache.  This time stamp will be 0
     *         if the project does not exist.
     */
    private long getLastAccessTime(ProjectId projectId) {
        Long timestamp;
        try {
            lastAccessLock.readLock().lock();
            timestamp = lastAccessMap.get(projectId);
        }
        finally {
            lastAccessLock.readLock().unlock();
        }
        return Objects.requireNonNullElse(timestamp, 0L);
    }

    private void logProjectAccess(final ProjectId projectId) {
        try {
            lastAccessLock.writeLock().lock();
            long currentTime = System.currentTimeMillis();
            int currentSize = lastAccessMap.size();
            lastAccessMap.put(projectId, currentTime);
            if(lastAccessMap.size() > currentSize) {
                logger.info("{} projects are now being accessed", lastAccessMap.size());
            }
        }
        finally {
            lastAccessLock.writeLock().unlock();
        }
    }

    @Override
    public void dispose() {
        purgeAllProjects();
    }
}
