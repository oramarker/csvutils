//import org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.serialization.record.RecordSchema;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;







def confluentSchemaRegistryNameValue = ConfluentSchemaRegistryName.value
//final ConfluentSchemaRegistry schemaService = context.getProperty(“ConfluentSchemaRegistry”).asControllerService(ConfluentSchemaRegistry.class);


def flowFile = session.create();

def lookup = context.controllerServiceLookup
def schemaServiceId = lookup.getControllerServiceIdentifiers(ControllerService).find { 
    cs -> lookup.getControllerServiceName(cs) == confluentSchemaRegistryNameValue
}
def schemaService = lookup.getControllerService(schemaServiceId)


// Service Lookup code in Java
 	 ControllerServiceLookup serviceLookup = context.getControllerServiceLookup();
     Set<String> serviceIds = serviceLookup.getControllerServiceIdentifiers(SchemaRegistry.class);
   

     String serviceNameToFind = confluentSchemaRegistryNameValue;
    	String serviceIdToFind = null;
    	for(String serviceId:serviceIds){
    		if(serviceNameToFind.equals( serviceLookup.getControllerServiceName(serviceId))){
    			serviceIdToFind = serviceId;
    		}
    	}
    	
    	if (serviceIdToFind != null){
    	    SchemaRegistry schemaRegistry = (SchemaRegistry)serviceLookup.getControllerService(serviceIdToFind);
             flowFie = session.putAttribute(flowFile, 'confluentSchemaRegistryFound' ,"Found"); 	
             RecordSchema recordSchema = schemaRegistry.retrieveSchema("myschema2");
    	     long avroSchemaId = recordSchema.getIdentifier().getIdentifier().getAsLong();
             flowFie = session.putAttribute(flowFile, 'avroschemaIdFoundByJava', String.valueOf(avroSchemaId)); 
    	}else{
             flowFie = session.putAttribute(flowFile, 'confluentSchemaRegistryFound' ,"Not Found"); 	
       }
// End of Service Lookup code in Java


RecordSchema recordSchema = schemaService.retrieveSchema("myschema2");   
   OptionalLong ol = recordSchema.getIdentifier().getIdentifier();
 



def schemaText = schemaService.retrieveSchemaText("myschema2");
//def schemaText = "texxxx"

def text = 'Hello world!'

// Cast a closure with an outputStream parameter to OutputStreamCallback
flowFile = session.write(flowFile, {outputStream ->
  outputStream.write(schemaText.getBytes())
} as OutputStreamCallback)

  if (ol.isPresent()){
       def id = ol.getAsLong();
       flowFie = session.putAttribute(flowFile, 'avro.schema.id' , String.valueOf(id)); 	
  }

session.transfer(flowFile, REL_SUCCESS)
