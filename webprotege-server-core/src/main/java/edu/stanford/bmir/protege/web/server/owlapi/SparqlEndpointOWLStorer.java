package edu.stanford.bmir.protege.web.server.owlapi;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;
import org.openrdf.model.impl.SimpleValueFactory;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sparql.SPARQLConnection;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.rio.RDFFormat;
import org.semanticweb.owlapi.formats.NQuadsDocumentFormat;
import org.semanticweb.owlapi.formats.NTriplesDocumentFormat;
import org.semanticweb.owlapi.io.OWLOntologyDocumentTarget;
import org.semanticweb.owlapi.io.StringDocumentTarget;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLDocumentFormat;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;
import org.semanticweb.owlapi.model.OWLStorer;
import org.semanticweb.owlapi.rio.RioNQuadsStorerFactory;

public class SparqlEndpointOWLStorer implements OWLStorer {

  IRI storeEndpoint;
  String tboxGraph;

  @Override
  public boolean canStoreOntology(@Nonnull OWLDocumentFormat owlDocumentFormat) {
    return owlDocumentFormat instanceof NQuadsDocumentFormat;
  }

  @Override
  public void storeOntology(@Nonnull OWLOntology owlOntology, @Nonnull IRI iri,
      @Nonnull OWLDocumentFormat owlDocumentFormat)
      throws OWLOntologyStorageException, IOException {

    SPARQLConnection con = (SPARQLConnection) getRepositoryConnection();

    org.openrdf.model.IRI tBoxGraphIri = SimpleValueFactory.getInstance().createIRI(
            StringUtils.removeEnd(storeEndpoint.toString(), "/") + "/"
                    + (tboxGraph.replaceAll("/", "") + "/"));

    String ont = prepareOntology(owlOntology, iri, tBoxGraphIri);
    con.clear(tBoxGraphIri);
    con.add(new ByteArrayInputStream(ont.getBytes()), storeEndpoint.toString(), RDFFormat.NQUADS);
    con.close();
  }

  private RepositoryConnection getRepositoryConnection() {
    SPARQLRepository repo = new SPARQLRepository(storeEndpoint.toString());
    repo.enableQuadMode(true);
    if (!repo.isInitialized()) {
      repo.initialize();
    }
    return repo.getConnection();
  }

  private String prepareOntology(OWLOntology owlOntology, IRI iri, org.openrdf.model.IRI tBoxGraphIri)
          throws IOException, OWLOntologyStorageException {
    StringDocumentTarget sdt = new StringDocumentTarget();
    NTriplesDocumentFormat format = new NTriplesDocumentFormat();

    this.storeOntology(owlOntology, sdt, format);

    String ont = sdt.toString();
    ont = ont.replace("_:", "<" + iri + "#");
    ont = ont.replace(" <http", "> <http");
    ont = ont.replace(" .", "> .");
    ont = ont.replace(">>", ">");
    ont = ont.replace("\">", "\"");
    //remove language tags because of some issues with language tagged literals during parsing via SPARQLConnection.add()
    ont = ont.replaceAll("@([a-z]{2})>", "");
    ont = ont.replaceAll("@([a-z]{2}-[A-Z]{2,3})>", "");
    ont = ont.replace(" .", " <" + tBoxGraphIri + "> .");

    return ont;
  }

  @Override
  public void storeOntology(@Nonnull OWLOntology owlOntology,
      @Nonnull OWLOntologyDocumentTarget owlOntologyDocumentTarget,
      @Nonnull OWLDocumentFormat owlDocumentFormat)
      throws OWLOntologyStorageException {
      owlOntology.saveOntology(owlDocumentFormat, owlOntologyDocumentTarget);
  }

  public void setEndpoint(IRI endpoint) {
    storeEndpoint = endpoint;
  }

  public void setTboxGraph(String graph) {
    tboxGraph = graph;
  }

}
