/**
** Converts avro binary to confluent kafka message 
*   
*/
def flowFileList = session.get(100)
if (flowFileList.isEmpty()) return

def flowFile = session.get();

String schemaIdStr = flowFile.getAttribute("confluent.schema.id");
int schemaId = Integer.valueOf(schemaIdStr)

byte MAGIC_BYTE = 0x0;
int idSize = 4;

flowFileList.each { flowFile ->
       try {
           String schemaIdStr = flowFile.getAttribute("confluent.schema.id");
           if ( schemaIdStr == null || schemaIdStr.trim() == "" ) {
             throw new RuntimeException ("No schema id found, attribute 'confluent.schema.id' not populated");
           }
           int schemaId = Integer.valueOf(schemaIdStr)

           def newFlowFile = session.write(flowFile,   { InputStream inputStream, OutputStream outputStream ->
                 outputStream.write(MAGIC_BYTE);
                 outputStream.write(ByteBuffer.allocate(idSize).putInt(schemaId).array());
                 org.apache.commons.io.IOUtils.copy(inputStream,outputStream,100*1024);
                } as StreamCallback
           );
           session.transfer(newFlowFile, REL_SUCCESS);
           session.remove(flowFile)
       } catch (Exception ex){
           log.error("Unable to convert avro to confluent kafka message.", ex)
           session.transfer(flowFile, REL_FAILURE);
       }
}

