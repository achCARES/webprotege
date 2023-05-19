package edu.stanford.bmir.protege.web.server.owlapi;

import javax.annotation.Nonnull;
import org.semanticweb.owlapi.formats.NQuadsDocumentFormat;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.util.OWLDocumentFormatFactoryImpl;

public class SparqlEndpointOWLStorerFactory implements OWLStorerFactory {

  IRI storeEndpoint;
  String tboxGraph;

  @Nonnull
  @Override
  public OWLStorer createStorer() {
    SparqlEndpointOWLStorer storer = new SparqlEndpointOWLStorer();
    storer.setEndpoint(storeEndpoint);
    storer.setTboxGraph(tboxGraph);
    return storer;
  }

  @Nonnull
  @Override
  public OWLDocumentFormatFactory getFormatFactory() {
    return new OWLDocumentFormatFactoryImpl() {
      @Override
      public OWLDocumentFormat createFormat() {
        return new NQuadsDocumentFormat();
      }
    };
  }

  @Override
  public OWLStorer get() {
    return createStorer();
  }

  public SparqlEndpointOWLStorerFactory setEndpoint(IRI endpoint) {
    storeEndpoint = endpoint;
    return this;
  }

  public SparqlEndpointOWLStorerFactory setTboxGraph(String graph) {
    tboxGraph = graph;
    return this;
  }

}
