import org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.serialization.record.RecordSchema;

def confluentSchemaRegistryNameValue = ConfluentSchemaRegistryName.value
//final ConfluentSchemaRegistry schemaService = context.getProperty(“ConfluentSchemaRegistry”).asControllerService(ConfluentSchemaRegistry.class);


def lookup = context.controllerServiceLookup
def schemaServiceId = lookup.getControllerServiceIdentifiers(ControllerService).find { 
    cs -> lookup.getControllerServiceName(cs) == confluentSchemaRegistryNameValue
}
def schemaService = lookup.getControllerService(schemaServiceId)

RecordSchema recordSchema = schemaService.retrieveSchema("myschema2");   
   OptionalLong ol = recordSchema.getIdentifier().getIdentifier();
 



def schemaText = schemaService.retrieveSchemaText("myschema2");
//def schemaText = "texxxx"

def text = 'Hello world!'
def flowFile = session.create();
// Cast a closure with an outputStream parameter to OutputStreamCallback
flowFile = session.write(flowFile, {outputStream ->
  outputStream.write(schemaText.getBytes())
} as OutputStreamCallback)

  if (ol.isPresent()){
       def id = ol.getAsLong();
       flowFie = session.putAttribute(flowFile, 'avro.schema.id' , String.valueOf(id)); 	
  }

session.transfer(flowFile, REL_SUCCESS)
