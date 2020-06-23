package com.cerner.bunsen.spark.serializable;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.spark.SparkRowConverter;
import java.io.IOException;
import java.io.Serializable;

/**
 * Abstract base class to support functions that need to be serialized
 * with a FHIR context for multi-threaded performance convenience.
 * Typically sub-classes would inherit from this and implement the
 * appropriate functional interface for the needed operation.
 */
public abstract class HasSerializableContext implements Serializable {

  protected FhirVersionEnum fhirVersion;

  protected transient FhirContext context;

  protected HasSerializableContext(FhirVersionEnum fhirVersion) {

    this.fhirVersion = fhirVersion;

    init();
  }

  private void writeObject(java.io.ObjectOutputStream stream) throws IOException {

    stream.defaultWriteObject();
  }

  private void readObject(java.io.ObjectInputStream stream) throws IOException,
      ClassNotFoundException {

    stream.defaultReadObject();

    init();
  }

  private void init() {

    context = FhirContexts.contextFor(fhirVersion);
  }

}
