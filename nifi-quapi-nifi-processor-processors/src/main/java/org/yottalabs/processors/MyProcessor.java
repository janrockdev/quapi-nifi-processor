package org.yottalabs.processors;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class MyProcessor extends AbstractProcessor {

//    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
//            .Builder().name("MY_PROPERTY")
//            .displayName("My property")
//            .description("Example Property")
//            .required(true)
//            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//            .build();
//
//    public static final Relationship MY_RELATIONSHIP = new Relationship.Builder()
//            .name("MY_RELATIONSHIP")
//            .description("Example relationship")
//            .build();
//
//    private List<PropertyDescriptor> descriptors;
//
//    private Set<Relationship> relationships;
//
//    @Override
//    protected void init(final ProcessorInitializationContext context) {
//        descriptors = new ArrayList<>();
//        descriptors.add(MY_PROPERTY);
//        descriptors = Collections.unmodifiableList(descriptors);
//
//        relationships = new HashSet<>();
//        relationships.add(MY_RELATIONSHIP);
//        relationships = Collections.unmodifiableSet(relationships);
//    }
//
//    @Override
//    public Set<Relationship> getRelationships() {
//        return this.relationships;
//    }
//
//    @Override
//    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
//        return descriptors;
//    }
//
//    @OnScheduled
//    public void onScheduled(final ProcessContext context) {
//
//    }
//
//    @Override
//    public void onTrigger(final ProcessContext context, final ProcessSession session) {
//        FlowFile flowFile = session.get();
//        if ( flowFile == null ) {
//            return;
//        }
//        // TODO implement
//    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowfile = session.get();

        if (flowfile == null) {
            context.yield();
            return;
        }

        /* ------------------------------------------------------------------------------------
         * Here's where the real work of a custom processor goes, usually containing a call to:
         * a) session.read( flowfile, new InputStreamCallback()...
         * b) session.write( flowfile, new OutputStreamCallback()... or
         * c) session.write( flowfile, new StreamCallback()...
         *
         * The original flowfile is read, maybe a new one is written to output.
         * ------------------------------------------------------------------------------------
         */

// how we reach nifi-app.log...
        getLogger().info("Just passing through on our way to Go...");

// how to get the value of a property...
        String favorite = context.getProperty(MONOPOLY_PROPERTY.getName()).getValue();

        if (!favorite.equals("Bordwalk")) {
            session.transfer(flowfile, FAILURE);
            return;
        }

// how to create and set a new flowfile attribute...
        flowfile = session.putAttribute(flowfile, "property", "Bordwalk");
        flowfile = session.putAttribute(flowfile, "content", "flowfile unchanged");
        session.transfer(flowfile, SUCCESS);
    }

    public static final PropertyDescriptor MONOPOLY_PROPERTY = new PropertyDescriptor.Builder()
            .name("Favorite property name")           // must always remain the same or flows using this processor will break!
            .displayName("Name") // this name can change without breaking flow.xml.gz
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("Bordwalk")
            .description("Favorite property name on the Monopoly board.")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("The flowfile passes go.")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("The flowfile doesn't pass go.")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MONOPOLY_PROPERTY);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = relationships;
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
}
