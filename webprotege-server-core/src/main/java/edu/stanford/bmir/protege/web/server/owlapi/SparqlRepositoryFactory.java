package edu.stanford.bmir.protege.web.server.owlapi;


import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.semanticweb.owlapi.model.IRI;

public class SparqlRepositoryFactory {

    IRI storeEndpoint;

    public SPARQLRepository createRepository() {
        SPARQLRepository repo = new SPARQLRepository(storeEndpoint.toString());
        repo.enableQuadMode(true);
        if (!repo.isInitialized()) {
            repo.init();
        }
        return repo;
    }

    public SPARQLRepository get() {
        return createRepository();
    }


    public SparqlRepositoryFactory setEndpoint(IRI endpoint) {
        storeEndpoint = endpoint;
        return this;
    }
}
