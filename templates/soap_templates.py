PORTABILITY_REQUEST_TEMPLATE = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:por="http://nc.aopm.es/v1-10/portabilidad" xmlns:v1="http://nc.aopm.es/v1-10" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
   <soapenv:Header/>
   <soapenv:Body>
      <por:peticionCrearSolicitudIndividualAltaPortabilidadMovil>
         <v1:codigoSesion>{session_code}</v1:codigoSesion>
         <por:fechaSolicitudPorAbonado>{request_date}</por:fechaSolicitudPorAbonado>
         <por:codigoOperadorDonante>{donor_operator}</por:codigoOperadorDonante>
         <por:codigoOperadorReceptor>{recipient_operator}</por:codigoOperadorReceptor>
         <por:abonado>
            <v1:documentoIdentificacion>
               <v1:tipo>{document_type}</v1:tipo>
               <v1:documento>{document_number}</v1:documento>
            </v1:documentoIdentificacion>
            <v1:datosPersonales xsi:type="v1:DatosPersonalesAbonadoPersonaFisica">
               <v1:nombre>{first_name}</v1:nombre>
               <v1:primerApellido>{first_surname}</v1:primerApellido>
               <v1:segundoApellido>{second_surname}</v1:segundoApellido>
               <v1:nacionalidad>{nationality}</v1:nacionalidad>
            </v1:datosPersonales>
         </por:abonado>
         <por:codigoContrato>{contract_code}</por:codigoContrato>
         <por:NRNReceptor>{nrn_receptor}</por:NRNReceptor>
         {fecha_ventana_optional}
         {iccid_optional}
         <por:MSISDN>{msisdn}</por:MSISDN>
      </por:peticionCrearSolicitudIndividualAltaPortabilidadMovil>
   </soapenv:Body>
</soapenv:Envelope>"""

CHECK_PORT_IN_STATUS_TEMPLATE = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:por="http://nc.aopm.es/v1-10/portabilidad" xmlns:v1="http://nc.aopm.es/v1-10">
   <soapenv:Header/>
   <soapenv:Body>
      <por:peticionConsultarProcesosPortabilidadMovil>
         <v1:codigoReferencia>{reference_code}</v1:codigoReferencia>
         <por:MSISDN>{msisdn}</por:MSISDN>
      </por:peticionConsultarProcesosPortabilidadMovil>
   </soapenv:Body>
  </soapenv:Envelope>
    """

GET_PORT_IN_REQUEST_TEMPLATE = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:por="http://nc.aopm.es/v1-10/portabilidad" xmlns:v1="http://nc.aopm.es/v1-10">
ObtenerSolicitudAltaPortabilidadMovil
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:por="http://nc.aopm.es/v1-10/portabilidad" xmlns:v1="http://nc.aopm.es/v1-10">
   <soapenv:Header/>
   <soapenv:Body>
      <por:peticionObtenerSolicitudAltaPortabilidadMovil>
         <v1:codigoSesion>{session_code}</v1:codigoSesion>
         <por:codigoReferencia>{reference_code}</por:codigoReferencia>
      </por:peticionObtenerSolicitudAltaPortabilidadMovil>
   </soapenv:Body>
</soapenv:Envelope>
"""

CANCEL_PORT_IN_REQUEST_TEMPLATE = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:por="http://nc.aopm.es/v1-10/portabilidad" xmlns:v1="http://nc.aopm.es/v1-10">
   <soapenv:Header/>
   <soapenv:Body>
      <por:peticionCancelarSolicitudAltaPortabilidadMovil>
         <v1:codigoSesion>{session_code}</v1:codigoSesion>
         <por:codigoReferencia>{reference_code}</por:codigoReferencia>
         <por:causaEstado>{cancellation_reason}</por:causaEstado>
         <por:cancelacionIniciadaPorDonante>{cancellation_initiated_by_donor}</por:cancelacionIniciadaPorDonante>
      </por:peticionCancelarSolicitudAltaPortabilidadMovil>
   </soapenv:Body>
</soapenv:Envelope>
"""
# ConsultarProcesosPortabilidadMovil
CONSULT_PROCESS_PORT_IN = """
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:por="http://nc.aopm.es/v1-10/portabilidad" xmlns:v1="http://nc.aopm.es/v1-10">
   <soapenv:Header/>
   <soapenv:Body>
      <por:peticionConsultarProcesosPortabilidadMovil>
         <v1:codigoSesion>{session_code}</v1:codigoSesion>
         <por:MSISDN>{msisdn}</por:MSISDN>
      </por:peticionConsultarProcesosPortabilidadMovil>
   </soapenv:Body>
</soapenv:Envelope>
"""

INITIATE_SESSION = """
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Header />
    <soapenv:Body>
        <peticionIniciarSesion xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://nc.aopm.es/v1-10/acceso">
            <codigoUsuario>{username}</codigoUsuario>
            <claveAcceso>{access_code}</claveAcceso>
            <codigoOperador>{operator_code}</codigoOperador>
        </peticionIniciarSesion>
    </soapenv:Body>
</soapenv:Envelope>
"""

PORTABILITY_REQUEST_TEMPLATE_1 = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:por="http://nc.aopm.es/v1-10/portabilidad" xmlns:v1="http://nc.aopm.es/v1-10">
   <soapenv:Header>
      <v1:sesion>{session_code}</v1:sesion>
   </soapenv:Header>
   <soapenv:Body>
      <por:peticionCrearSolicitudIndividualAltaPortabilidadMovil>
         <por:fechaSolicitudPorAbonado>{request_date}</por:fechaSolicitudPorAbonado>
         <por:codigoOperadorDonante>{donor_operator}</por:codigoOperadorDonante>
         <por:codigoOperadorReceptor>{recipient_operator}</por:codigoOperadorReceptor>
         <por:abonado>
            <v1:documentoIdentificacion>
               <v1:tipo>{id_type}</v1:tipo>
               <v1:documento>{id_number}</v1:documento>
            </v1:documentoIdentificacion>
            <v1:datosPersonales>
               <v1:nombre>{first_name}</v1:nombre>
               <v1:primerApellido>{first_surname}</v1:primerApellido>
               <v1:segundoApellido>{second_surname}</v1:segundoApellido>
            </v1:datosPersonales>
         </por:abonado>
         <por:codigoContrato>{contract_code}</por:codigoContrato>
         <por:NRNReceptor>{nrn_receptor}</por:NRNReceptor>
         {fecha_ventana_optional}
         {iccid_optional}
         <por:MSISDN>{msisdn}</por:MSISDN>
      </por:peticionCrearSolicitudIndividualAltaPortabilidadMovil>
   </soapenv:Body>
</soapenv:Envelope>"""