package edu.stanford.bmir.protege.web.server.owlapi.change;

import com.google.common.base.*;
import static com.google.common.base.Objects.*;

import com.google.common.base.Objects;
import edu.stanford.bmir.protege.web.shared.revision.RevisionNumber;
import edu.stanford.bmir.protege.web.shared.user.UserId;
import org.semanticweb.owlapi.change.*;


import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Author: Matthew Horridge<br>
 * Stanford University<br>
 * Bio-Medical Informatics Research Group<br>
 * Date: 18/07/2013
 */
public class Revision implements Iterable<OWLOntologyChangeRecord>, Comparable<Revision> {


    private final UserId userId;

    private final RevisionNumber revisionNumber;

    private long timestamp;

    private final OWLOntologyChangeRecordList changes;

    private final String highLevelDescription;

    public Revision(UserId userId, RevisionNumber revisionNumber, OWLOntologyChangeRecordList changes, long timestamp, String highLevelDescription) {
        this.changes = checkNotNull(changes);
        this.userId = checkNotNull(userId);
        this.revisionNumber = checkNotNull(revisionNumber);
        this.timestamp = timestamp;
        this.highLevelDescription = checkNotNull(highLevelDescription);
    }

    public int getSize() {
        return changes.size();
    }

    public static Revision createEmptyRevisionWithRevisionNumber(RevisionNumber revision) {
        return new Revision(UserId.getGuest(), revision, new OWLOntologyChangeRecordList(Collections.<OWLOntologyChangeRecord>emptyList()), 0l, "");
    }

    public long getTimestamp() {
        return timestamp;
    }

    public UserId getUserId() {
        return userId;
    }

    public RevisionNumber getRevisionNumber() {
        return revisionNumber;
    }

    public int compareTo(Revision o) {
        return this.revisionNumber.compareTo(o.revisionNumber);
    }

    public String getHighLevelDescription() {
        return highLevelDescription != null ? highLevelDescription : "";
    }

    public Iterator<OWLOntologyChangeRecord> iterator() {
        return changes.iterator();
    }


    @Override
    public String toString() {
        return toStringHelper("Revision")
                .addValue(revisionNumber)
                .addValue(userId)
                .add("timestamp", timestamp)
                .add("description", highLevelDescription)
                .addValue(changes)
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Revision)) {
            return false;
        }
        Revision other = (Revision) obj;
        return this.userId.equals(other.userId)
                && this.revisionNumber.equals(other.revisionNumber)
                && this.timestamp == other.timestamp
                && this.highLevelDescription.equals(other.highLevelDescription)
                && this.changes.equals(other.changes);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(userId, revisionNumber, timestamp, highLevelDescription, changes);
    }
}
