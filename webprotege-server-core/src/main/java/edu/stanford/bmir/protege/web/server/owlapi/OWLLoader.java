package edu.stanford.bmir.protege.web.server.owlapi;

import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;

import javax.annotation.Nonnull;
import java.io.Serializable;

public interface OWLLoader extends Serializable {

    public OWLOntology LoadOntology(@Nonnull IRI iri);


}
