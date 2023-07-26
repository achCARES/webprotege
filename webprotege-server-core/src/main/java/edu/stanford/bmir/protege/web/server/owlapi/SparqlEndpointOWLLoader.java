package edu.stanford.bmir.protege.web.server.owlapi;

import org.apache.commons.lang.StringUtils;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Iri;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.formats.NTriplesDocumentFormat;
import org.semanticweb.owlapi.io.StringDocumentSource;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import javax.annotation.Nonnull;
import java.text.Normalizer;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;

import static org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder.from;
import static org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder.var;

public class SparqlEndpointOWLLoader implements OWLLoader {
    IRI storeEndpoint;
    String tboxGraph;

    @Override
    public OWLOntology LoadOntology(@Nonnull IRI iri) {
        OWLOntologyManager owlOntologyManager = OWLManager.createOWLOntologyManager();

        Iri tBoxGraphIri = Rdf.iri(
                StringUtils.removeEnd(storeEndpoint.toString(), "/") + "/"
                        + (tboxGraph + "/").replace("//", "/"));

        List<BindingSet> results = getAllQueryResultsFomGraph(iri, tBoxGraphIri);

        String triples = convertQueryResultsToNtriples(results);

        try {
            owlOntologyManager.loadOntologyFromOntologyDocument(
                    new StringDocumentSource(triples, iri, new NTriplesDocumentFormat(), "application/n-triples"));
        } catch (OWLOntologyCreationException e) {
            throw new RuntimeException(e);
        }

        return owlOntologyManager.getOntologies().iterator().next();
    }

    public void setEndpoint(IRI endpoint) {
        storeEndpoint = endpoint;
    }

    public void setTboxGraph(String graph) {
        tboxGraph = graph;
    }

    private List<BindingSet> getAllQueryResultsFomGraph(IRI endpoint, Iri graph) {
        SPARQLRepository repo = new SparqlRepositoryFactory().setEndpoint(endpoint).get();

        TriplePattern spo = GraphPatterns.tp(var("s"), var("p"), var("o"));

        SelectQuery selectQuery = Queries.SELECT()
                .base(Rdf.iri(endpoint.toString()))
                .all()
                .from(from(graph))
                .where(spo);

        return Repositories.tupleQuery(repo, selectQuery.getQueryString(), r -> QueryResults.asList(r));
    }

    private String convertQueryResultsToNtriples(List<BindingSet> results) {
        String triples = "";

        Iterator it = results.iterator();

        while (it.hasNext()) {
            BindingSet binding = (BindingSet) it.next();

            String s = getNtripleValueFromBinding(binding.getBinding("s").getValue());
            String p = getNtripleValueFromBinding(binding.getBinding("p").getValue());
            String o = getNtripleValueFromBinding(binding.getBinding("o").getValue());

            triples = triples.concat(s + " " + p + " " + o + " .\n");
        }

        return triples;
    }

    private String getNtripleValueFromBinding(Value bindingValue) {
        String ntripleValue;
        String stringValue = bindingValue.stringValue();

        if (bindingValue.isResource()) {
            if (stringValue.contains(SparqlEndpointOWLStorer.PREF_SKOLEM)) {
                int index = stringValue.lastIndexOf('#');
                ntripleValue = stringValue.substring(index +1).replace(SparqlEndpointOWLStorer.PREF_SKOLEM, "_:");
            } else {
                ntripleValue = "<" + stringValue + ">";
            }
        } else {
            ntripleValue = "\"" +  Normalizer.normalize(
                    stringValue.replaceAll("\"", Matcher.quoteReplacement("\\\"")), Normalizer.Form.NFKC) + "\"";
        }
        return ntripleValue;
    }

}
