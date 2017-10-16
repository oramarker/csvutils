



byte MAGIC_BYTE = 0x0;
 int idSize = 4;

ByteArrayOutputStream out = new ByteArrayOutputStream();
out.write(MAGIC_BYTE);
out.write(ByteBuffer.allocate(idSize).putInt(id).array());
