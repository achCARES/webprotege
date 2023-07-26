package edu.stanford.bmir.protege.web.server.owlapi;

import org.semanticweb.owlapi.model.IRI;
import javax.annotation.Nonnull;

public class SparqlEndpointOWLLoaderFactory implements OWLLoaderFactory {
    IRI storeEndpoint;
    String tboxGraph;

    @Nonnull
    @Override
    public OWLLoader createLoader() {
        SparqlEndpointOWLLoader loader = new SparqlEndpointOWLLoader();
        loader.setEndpoint(storeEndpoint);
        loader.setTboxGraph(tboxGraph);
        return loader;
    }
    @Nonnull
    @Override
    public OWLLoader get() {
        return createLoader();
    }

    public SparqlEndpointOWLLoaderFactory setEndpoint(IRI endpoint) {
        storeEndpoint = endpoint;
        return this;
    }

    public SparqlEndpointOWLLoaderFactory setTboxGraph(String graph) {
        tboxGraph = graph;
        return this;
    }

}
