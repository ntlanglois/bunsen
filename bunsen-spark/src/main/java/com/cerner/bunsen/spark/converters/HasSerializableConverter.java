package com.cerner.bunsen.spark.converters;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.spark.SparkRowConverter;
import com.cerner.bunsen.spark.serializable.HasSerializableContext;
import java.io.IOException;
import java.io.Serializable;

/**
 * Abstract base class to support functions that need to be serialized
 * with a a row converter. Typically sub-classes would inherit from
 * this and implement the appropriate functional interface for the
 * needed operation.  For multi-threaded performance convenience, the
 * FHIR context is also exposed to sub-classes.
 */
public abstract class HasSerializableConverter extends HasSerializableContext {

  private String resourceTypeUrl;

  protected transient SparkRowConverter converter;

  protected HasSerializableConverter(String resourceTypeUrl,
      FhirVersionEnum fhirVersion) {

    super(fhirVersion);

    this.resourceTypeUrl = resourceTypeUrl;

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

    converter = SparkRowConverter.forResource(context, resourceTypeUrl);
  }

}
