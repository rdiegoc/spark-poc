package exeo.spark_poc

import java.util.Date
import java.math.BigDecimal
import org.apache.commons.lang3.time.DateFormatUtils

/**
 * @author DIEGO
 */
class RecargaData () extends Serializable {
  
  var idMayorista : Long = 0;
  var fechaTransTp : Date = null;
  var montoRecarga : BigDecimal = null;
  
}