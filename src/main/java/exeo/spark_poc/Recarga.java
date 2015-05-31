/**
 * 
 */
package exeo.spark_poc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author Pablo
 *
 */
public class Recarga implements Serializable{

	private BigDecimal id;
	private Long idMayorista;
	private Long idSubred;
	private String idPdvMay;
	private String idPdvTp;
	private String tidPdv;
	private Date fechaTransPdv;
	private String codTerminal;
	private String tidTp;
	private Date fechaTransTp;
	private String nroLinea;
	private String plataformaRecarga;
	private BigDecimal montoRecarga;
	private String monedaRecarga;
	private BigDecimal saldo;
	private Date fechaRevPdv;
	private Date fechaRevTp;
	private Date fechaUltMod;
	private String tipoProducto;
	private String estado;
	private String descripcionEstado;
	
	
	public String getEstado() {
		return estado;
	}

	public void setEstado(String estado) {
		this.estado = estado;
	}

	public String getDescripcionEstado() {
		return descripcionEstado;
	}

	public void setDescripcionEstado(String descripcionEstado) {
		this.descripcionEstado = descripcionEstado;
	}

	public BigDecimal getId() {

		return this.id;
	}

	public void setId(BigDecimal id) {
		this.id = id;		
	}

	/**
	 * @return the idMayorista
	 */
	public Long getIdMayorista() {
		return idMayorista;
	}

	/**
	 * @param idMayorista the idMayorista to set
	 */
	public void setIdMayorista(Long idMayorista) {
		this.idMayorista = idMayorista;
	}

	/**
	 * @return the idSubred
	 */
	public Long getIdSubred() {
		return idSubred;
	}

	/**
	 * @param idSubred the idSubred to set
	 */
	public void setIdSubred(Long idSubred) {
		this.idSubred = idSubred;
	}



	public String getIdPdvMay() {
		return idPdvMay;
	}

	public void setIdPdvMay(String idPdvMay) {
		this.idPdvMay = idPdvMay;
	}

	public String getIdPdvTp() {
		return idPdvTp;
	}

	public void setIdPdvTp(String idPdvTp) {
		this.idPdvTp = idPdvTp;
	}

	/**
	 * @return the tidPdv
	 */
	public String getTidPdv() {
		return tidPdv;
	}

	/**
	 * @param tidPdv the tidPdv to set
	 */
	public void setTidPdv(String tidPdv) {
		this.tidPdv = tidPdv;
	}

	/**
	 * @return the fechaTransPdv
	 */
	public Date getFechaTransPdv() {
		return fechaTransPdv;
	}

	/**
	 * @param fechaTransPdv the fechaTransPdv to set
	 */
	public void setFechaTransPdv(Date fechaTransPdv) {
		this.fechaTransPdv = fechaTransPdv;
	}

	/**
	 * @return the codTerminal
	 */
	public String getCodTerminal() {
		return codTerminal;
	}

	/**
	 * @param codTerminal the codTerminal to set
	 */
	public void setCodTerminal(String codTerminal) {
		this.codTerminal = codTerminal;
	}

	/**
	 * @return the tidTp
	 */
	public String getTidTp() {
		return tidTp;
	}

	/**
	 * @param tidTp the tidTp to set
	 */
	public void setTidTp(String tidTp) {
		this.tidTp = tidTp;
	}

	/**
	 * @return the fechaTransTp
	 */
	public Date getFechaTransTp() {
		return fechaTransTp;
	}

	/**
	 * @param fechaTransTp the fechaTransTp to set
	 */
	public void setFechaTransTp(Date fechaTransTp) {
		this.fechaTransTp = fechaTransTp;
	}

	/**
	 * @return the nroLinea
	 */
	public String getNroLinea() {
		return nroLinea;
	}

	/**
	 * @param nroLinea the nroLinea to set
	 */
	public void setNroLinea(String nroLinea) {
		this.nroLinea = nroLinea;
	}

	/**
	 * @return the plataformaRecarga
	 */
	public String getPlataformaRecarga() {
		return plataformaRecarga;
	}

	/**
	 * @param plataformaRecarga the plataformaRecarga to set
	 */
	public void setPlataformaRecarga(String plataformaRecarga) {
		this.plataformaRecarga = plataformaRecarga;
	}

	/**
	 * @return the montoRecarga
	 */
	public BigDecimal getMontoRecarga() {
		return montoRecarga;
	}

	/**
	 * @param montoRecarga the montoRecarga to set
	 */
	public void setMontoRecarga(BigDecimal montoRecarga) {
		this.montoRecarga = montoRecarga;
	}

	/**
	 * @return the monedaRecarga
	 */
	public String getMonedaRecarga() {
		return monedaRecarga;
	}

	/**
	 * @param monedaRecarga the monedaRecarga to set
	 */
	public void setMonedaRecarga(String monedaRecarga) {
		this.monedaRecarga = monedaRecarga;
	}

	/**
	 * @return the saldo
	 */
	public BigDecimal getSaldo() {
		return saldo;
	}

	/**
	 * @param saldo the saldo to set
	 */
	public void setSaldo(BigDecimal saldo) {
		this.saldo = saldo;
	}

	/**
	 * @return the fechaRevPdv
	 */
	public Date getFechaRevPdv() {
		return fechaRevPdv;
	}

	/**
	 * @param fechaRevPdv the fechaRevPdv to set
	 */
	public void setFechaRevPdv(Date fechaRevPdv) {
		this.fechaRevPdv = fechaRevPdv;
	}

	/**
	 * @return the fechaRevTp
	 */
	public Date getFechaRevTp() {
		return fechaRevTp;
	}

	/**
	 * @param fechaRevTp the fechaRevTp to set
	 */
	public void setFechaRevTp(Date fechaRevTp) {
		this.fechaRevTp = fechaRevTp;
	}

	public Date getFechaUltMod() {
		return fechaUltMod;
	}

	public void setFechaUltMod(Date fechaUltMod) {
		this.fechaUltMod = fechaUltMod;
	}

	public String getTipoProducto() {
		return tipoProducto;
	}

	public void setTipoProducto(String tipoProducto) {
		this.tipoProducto = tipoProducto;
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this).append("idMayorista", this.idMayorista)
				.append("monto", this.montoRecarga)
				.append("fechaRecarga", this.fechaTransTp).toString();
	}

}
