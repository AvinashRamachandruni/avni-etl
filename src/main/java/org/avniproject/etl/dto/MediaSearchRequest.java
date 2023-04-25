package org.avniproject.etl.dto;

import java.util.Date;
import java.util.List;
import java.util.Objects;

public final class MediaSearchRequest {
    private List<String> subjectTypeNames;
    private List<String> programNames;
    private List<String> encounterTypeNames;
    private List<String> imageConcepts;
    private List<SyncValues> syncValues;
    private Date fromDate;
    private Date toDate;

    public MediaSearchRequest() {
    }

    public List<String> getSubjectTypeNames() {
        return subjectTypeNames;
    }

    public void setSubjectTypeNames(List<String> subjectTypeNames) {
        this.subjectTypeNames = subjectTypeNames;
    }

    public List<String> getProgramNames() {
        return programNames;
    }

    public void setProgramNames(List<String> programNames) {
        this.programNames = programNames;
    }

    public List<String> getEncounterTypeNames() {
        return encounterTypeNames;
    }

    public void setEncounterTypeNames(List<String> encounterTypeNames) {
        this.encounterTypeNames = encounterTypeNames;
    }

    public List<String> getImageConcepts() {
        return imageConcepts;
    }

    public void setImageConcepts(List<String> imageConcepts) {
        this.imageConcepts = imageConcepts;
    }

    public List<SyncValues> getSyncValues() {
        return syncValues;
    }

    public void setSyncValues(List<SyncValues> syncValues) {
        this.syncValues = syncValues;
    }

    public Date getFromDate() {
        return fromDate;
    }

    public void setFromDate(Date fromDate) {
        this.fromDate = fromDate;
    }

    public Date getToDate() {
        return toDate;
    }

    public void setToDate(Date toDate) {
        this.toDate = toDate;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (MediaSearchRequest) obj;
        return Objects.equals(this.subjectTypeNames, that.subjectTypeNames) &&
                Objects.equals(this.programNames, that.programNames) &&
                Objects.equals(this.encounterTypeNames, that.encounterTypeNames) &&
                Objects.equals(this.imageConcepts, that.imageConcepts) &&
                Objects.equals(this.syncValues, that.syncValues) &&
                Objects.equals(this.fromDate, that.fromDate) &&
                Objects.equals(this.toDate, that.toDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subjectTypeNames, programNames, encounterTypeNames, imageConcepts, syncValues, fromDate, toDate);
    }

    @Override
    public String toString() {
        return "MediaSearchRequest[" +
                "subjectTypeNames=" + subjectTypeNames + ", " +
                "programNames=" + programNames + ", " +
                "encounterTypeNames=" + encounterTypeNames + ", " +
                "imageConcepts=" + imageConcepts + ", " +
                "syncValues=" + syncValues + ", " +
                "fromDate=" + fromDate + ", " +
                "toDate=" + toDate + ']';
    }
}
