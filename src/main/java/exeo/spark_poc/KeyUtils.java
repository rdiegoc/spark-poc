package exeo.spark_poc;

import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;

public class KeyUtils {

	public static String generateKey(Long idMayorista) {
		if(idMayorista == null)
			return "null";
		
		return generateKeyFor(idMayorista.toString());
	}
	
	public static String generateKey(Recarga recarga) {
		if(recarga == null)
			return "null";
		
		return generateKeyFor(recarga);
	}

	public static String generateKeyFor(String string) {
		if(string == null)
			return "null";
		
		String format = DateFormatUtils.format(new Date(), "yyyyMMdd:HH");
		
		StringBuilder key = new StringBuilder("rol:")
			.append(format).append(":").append(string);
		return key.toString();
	}
	
	private static String generateKeyFor(Recarga recarga) {
		String format = DateFormatUtils.format(recarga.getFechaTransTp(), "yyyyMMdd:HH");
		
		StringBuilder key = new StringBuilder(format)
			.append(":").append(recarga.getIdMayorista());
		return key.toString();
	}

}
