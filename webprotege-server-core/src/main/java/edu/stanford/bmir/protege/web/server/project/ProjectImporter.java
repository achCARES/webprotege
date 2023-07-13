package edu.stanford.bmir.protege.web.server.project;


import com.google.auto.factory.AutoFactory;
import com.google.auto.factory.Provided;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import edu.stanford.bmir.protege.web.server.change.*;
import edu.stanford.bmir.protege.web.server.owlapi.SparqlEndpointOWLStorerFactory;
import edu.stanford.bmir.protege.web.server.revision.Revision;
import edu.stanford.bmir.protege.web.server.revision.RevisionStoreFactory;
import edu.stanford.bmir.protege.web.server.upload.DocumentResolver;
import edu.stanford.bmir.protege.web.server.upload.UploadedOntologiesProcessor;
import edu.stanford.bmir.protege.web.server.util.MemoryMonitor;
import edu.stanford.bmir.protege.web.shared.csv.DocumentId;
import edu.stanford.bmir.protege.web.shared.project.ProjectId;
import edu.stanford.bmir.protege.web.shared.revision.RevisionNumber;
import edu.stanford.bmir.protege.web.shared.user.UserId;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.formats.NQuadsDocumentFormat;
import org.semanticweb.owlapi.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 03/06/15
 */
public class ProjectImporter {

    private static final Logger logger = LoggerFactory.getLogger(ProjectImporter.class);

    @Nonnull
    private final UploadedOntologiesProcessor uploadedOntologiesProcessor;

    @Nonnull
    private final DocumentResolver documentResolver;

    @Nonnull
    private final ProjectId projectId;

    @Nonnull
    private final RevisionStoreFactory revisionStoreFactory;

    @AutoFactory
    @Inject
    public ProjectImporter(ProjectId projectId,
                           @Provided @Nonnull UploadedOntologiesProcessor uploadedOntologiesProcessor,
                           @Provided @Nonnull DocumentResolver documentResolver,
                           @Provided @Nonnull RevisionStoreFactory revisionStoreFactory) {
        this.projectId = checkNotNull(projectId);
        this.uploadedOntologiesProcessor = checkNotNull(uploadedOntologiesProcessor);
        this.documentResolver = checkNotNull(documentResolver);
        this.revisionStoreFactory = checkNotNull(revisionStoreFactory);
    }


    public void createProjectFromSources(DocumentId sourcesId,
                                         UserId owner, IRI sparqlEndpoint, String tboxGraph) throws IOException, OWLOntologyCreationException {
        logger.info("{} Creating project from sources", projectId);
        var stopwatch = Stopwatch.createStarted();
        var uploadedOntologies = uploadedOntologiesProcessor.getUploadedOntologies(sourcesId);
        logger.info("{} Loaded sources in {} ms", projectId, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        var memoryMonitor = new MemoryMonitor(logger);
        memoryMonitor.logMemoryUsage();
        logger.info("{} Writing change log", projectId);
        generateInitialChanges(owner, uploadedOntologies, sparqlEndpoint, tboxGraph);
        deleteSourceFile(sourcesId);
        logger.info("{} Project creation from sources complete in {} ms", projectId, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        memoryMonitor.logMemoryUsage();

    }

    private void generateInitialChanges(UserId owner, Collection<Ontology> uploadedOntologies, IRI sparqlEndpoint, String tboxGraph) {
        ImmutableList<OntologyChange> changeRecords = getInitialChangeRecords(uploadedOntologies);
        logger.info("{} Writing initial revision containing {} change records", projectId, changeRecords.size());
        Stopwatch stopwatch = Stopwatch.createStarted();
        var revisionStore = revisionStoreFactory.createRevisionStore(projectId);
        revisionStore.addRevision(
                new Revision(
                        owner,
                        RevisionNumber.getRevisionNumber(1),
                        changeRecords,
                        System.currentTimeMillis(),
                        "Initial import"));
        logger.info("{} Initial revision written in {} ms", projectId, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        uploadOntologiesToSparqlEnpoint(uploadedOntologies, sparqlEndpoint, tboxGraph);
    }

    private void uploadOntologiesToSparqlEnpoint(Collection<Ontology> uploadedOntologies, IRI sparqlEndpoint, String tboxGraph) {
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        OWLStorer storer = new SparqlEndpointOWLStorerFactory().setEndpoint(sparqlEndpoint).setTboxGraph(tboxGraph).get();

        try {
            for (Ontology o : uploadedOntologies) {
                NQuadsDocumentFormat format = new NQuadsDocumentFormat();
                try {
                    IRI oiri = o.getOntologyId().getOntologyIRI().get();
                    OWLOntology owl = manager.createOntology(o.getAxioms());
                    OWLOntologyID owlId = new OWLOntologyID(oiri);
                    SetOntologyID setOntologyID = new SetOntologyID(owl, owlId);
                    manager.applyChange(setOntologyID);
                    manager.setOntologyDocumentIRI(owl, oiri);
                    storer.storeOntology(owl, oiri, format);
                } catch (OWLOntologyCreationException | OWLOntologyStorageException | IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ImmutableList<OntologyChange> getInitialChangeRecords(Collection<Ontology> ontologies) {
        ImmutableList.Builder<OntologyChange> changeRecordList = ImmutableList.builder();
        for (var ont : ontologies) {
            var axioms = ont.getAxioms();
            logger.info("{} Processing ontology source ({} axioms)", projectId, axioms.size());
            for (var axiom : ont.getAxioms()) {
                changeRecordList.add(AddAxiomChange.of(ont.getOntologyId(), axiom));
            }
            for (var annotation : ont.getAnnotations()) {
                changeRecordList.add(AddOntologyAnnotationChange.of(ont.getOntologyId(), annotation));
            }
            for (var importsDeclaration : ont.getImportsDeclarations()) {
                changeRecordList.add(AddImportChange.of(ont.getOntologyId(), importsDeclaration));
            }
        }
        return changeRecordList.build();
    }

    private void deleteSourceFile(DocumentId sourceFileId) {
        var sourceFilePath = documentResolver.resolve(sourceFileId);
        try {
            Files.deleteIfExists(sourceFilePath);
        } catch(IOException e) {
            logger.info("Could not delete uploaded file: {} Cause: {}", sourceFilePath, e.getMessage());
        }
    }
}
