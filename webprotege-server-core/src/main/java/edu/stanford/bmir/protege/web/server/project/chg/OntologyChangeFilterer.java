package edu.stanford.bmir.protege.web.server.project.chg;

import edu.stanford.bmir.protege.web.server.change.OntologyChange;
import edu.stanford.bmir.protege.web.server.project.ProjectCache;
import org.semanticweb.owlapi.model.OWLAnnotation;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OntologyChangeFilterer {

    private static final Logger logger = LoggerFactory.getLogger(ProjectCache.class);

    public List<OntologyChange> filterOutChangesDifferentOnlyByBlankNodeId(List<OntologyChange> changes, OWLOntology ontology) {
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

}
