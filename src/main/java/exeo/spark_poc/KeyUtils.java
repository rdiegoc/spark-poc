package exeo.spark_poc;

import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;

public class KeyUtils {

	public static String generateKey(Long idMayorista) {
		if(idMayorista == null)
			return "null";
		
		return generateKeyFor(idMayorista.toString());
	}

	public static String generateKeyFor(String string) {
		if(string == null)
			return "null";
		
		String format = DateFormatUtils.format(new Date(), "yyyyMMdd");
		
		StringBuilder key = new StringBuilder(format)
			.append(":").append(string);
		return key.toString();
	}

}
