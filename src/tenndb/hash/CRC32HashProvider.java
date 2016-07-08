package tenndb.hash;

import java.util.zip.CRC32;

public class CRC32HashProvider {

	private final ThreadLocal<CRC32> _crc32 = new ThreadLocal<CRC32>() {
		protected CRC32 initialValue(){
			return new CRC32();
		}
	};

	public long hash(String key) {
		CRC32 crc = _crc32.get();
		crc.update(key.getBytes());
		return crc.getValue();
	}
	
}
