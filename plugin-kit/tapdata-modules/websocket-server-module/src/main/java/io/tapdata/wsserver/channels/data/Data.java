package io.tapdata.wsserver.channels.data;


public abstract class Data extends BinaryCodec {
    public static final int CODE_SUCCESS = 1;
    public static final int CODE_FAILED = 0;

    private byte type;

    public Data(byte type) {
        this.type = type;
    }

    public String getId(){
        return null;
    }
//	
//	void fromHailPack(HailPack pack) throws CoreException {
//		if(type != pack.getType())
//			throw new IllegalArgumentException("Incompatible Data type, expected " + type + ", but " + pack.getType());
//		short encode = pack.getEncode();
//		byte[] data = pack.getContent();
//		if(data == null)
//			throw new NullPointerException("HailPack's content is null, " + this);
//		try {
//			switch(encode) {
//			case HailPack.ENCODE_JSON:
//				String str = new String(data, "utf8");
////				LoggerEx.info(TAG, "Data received: " + str);
//				Document dbObj = Document.parse(str);
//				fromDBObjectPrivate(dbObj);
//				break;
//			case HailPack.ENCODE_JSON_GZIP:
//				GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(data));
//				ByteArrayOutputStream baos = new ByteArrayOutputStream();
//				byte[] buffer = new byte[8192];
//				int size;
//				while((size = gis.read(buffer)) != -1){
//					if(size > 0){
//						baos.write(buffer, 0, size);
//					}
//				}
//				String gzipStr = new String(baos.toByteArray(), "utf8");
//				LoggerEx.info(TAG, "Data received(gzip): " + gzipStr);
//				Document gzipDbObj = Document.parse(gzipStr);
//				fromDBObjectPrivate(gzipDbObj);
//				break;
//			default:
//				throw new CoreException(CoreErrorCodes.ERROR_TCPCHANNEL_ENCODE_ILLEGAL, "Unsupported HailPack decode, " + encode + " data " + this);
//			}
//		} catch(IOException e) {
//			LoggerEx.error(TAG, "Resurrect from Hail Pack " + pack + " failed, " + e.getMessage());
//			throw new CoreException(CoreErrorCodes.ERROR_HAILPACK_IO_ERROR, "Resurrect from Hail Pack failed, " + e.getMessage());
//		} catch(Throwable t) {
//			LoggerEx.error(TAG, "Resurrect from Hail Pack " + pack + " failed (Unknown), " + t.getMessage());
//			throw new CoreException(CoreErrorCodes.ERROR_HAILPACK_UNKNOWNERROR, "Resurrect from Hail Pack failed (Unknown), " + t.getMessage());
//		}
//	}
//	
//	HailPack toHailPack(short encode) throws CoreException {
//		try {
//			HailPack pack = new HailPack();
//			byte[] data;
//			switch(encode) {
//			case HailPack.ENCODE_JSON:
//				Document dbObj = toDocument();
//				data = dbObj.toJson(CommonUtils.MONGODB_JSONSETTINGS).getBytes("utf8");
//				break;
//			case HailPack.ENCODE_JSON_GZIP:
//				Document gzipDbObj = toDocument();
//				String str = gzipDbObj.toJson(CommonUtils.MONGODB_JSONSETTINGS);
//				ByteArrayOutputStream gzipBaos = new ByteArrayOutputStream(str.length());
//				GZIPOutputStream gos = new GZIPOutputStream(gzipBaos);
//				byte[] d = str.getBytes("utf8");
//				gos.write(d);
//				gos.close();
//				data = gzipBaos.toByteArray();
//				break;
//			default:
//				throw new IllegalArgumentException("Unsupported HailPack encode, " + encode);
//			}
//			pack.setContent(data);
//			pack.setLength(data.length);
//			pack.setEncode(encode);
//			pack.setType(type);
//			return pack;
//		} catch(IOException e) {
//			LoggerEx.error(TAG, "Persistent to Hail Pack " + this + " failed, " + e.getMessage());
//			throw new CoreException(CoreErrorCodes.ERROR_HAILPACK_IO_ERROR, "Persistent to Hail Pack failed, " + e.getMessage());
//		} catch(Throwable t) {
//			LoggerEx.error(TAG, "Persistent to Hail Pack " + this + " failed (Unknown), " + t.getMessage());
//			throw new CoreException(CoreErrorCodes.ERROR_HAILPACK_UNKNOWNERROR, "Persistent to Hail Pack failed (Unknown), " + t.getMessage());
//		}
//	}
//	
    /**
     * @param type the type to set
     */
    public void setType(byte type) {
        this.type = type;
    }

    /**
     * @return the type
     */
    public byte getType() {
        return type;
    }
}
