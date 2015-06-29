package exeo.spark_poc

import java.util.Date
import java.math.BigDecimal
import org.apache.commons.lang3.time.DateFormatUtils

/**
 * @author DIEGO
 */
class RecargaData () {
  
  var idMayorista : Long = 0;
  var fechaTransTp : Date = null;
  var montoRecarga : BigDecimal = null;
  
  def generateKey : String = {
    if(idMayorista == null)
      return "null";
    
    
    val format = DateFormatUtils.format(new Date(), "yyyyMMdd:HH");
    val key = new StringBuilder(format)
      .append(":").append(idMayorista);
    return key.toString();
  }
  
}