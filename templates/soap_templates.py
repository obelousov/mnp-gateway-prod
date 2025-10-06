PORTABILITY_REQUEST_TEMPLATE = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:por="http://nc.aopm.es/v1-10/portabilidad" xmlns:v1="http://nc.aopm.es/v1-10">
   <soapenv:Header/>
   <soapenv:Body>
      <por:peticionCrearSolicitudIndividualAltaPortabilidadMovil>
         <v1:codigoSesion>{session_code}</v1:codigoSesion>
         <por:fechaSolicitudPorAbonado>{request_date}</por:fechaSolicitudPorAbonado>
         <por:codigoOperadorDonante>{donor_operator}</por:codigoOperadorDonante>
         <por:codigoOperadorReceptor>{recipient_operator}</por:codigoOperadorReceptor>
         <por:abonado>
            <v1:documentoIdentificacion>
               <v1:tipo>{id_type}</v1:tipo>
               <v1:documento>{id_number}</v1:documento>
            </v1:documentoIdentificacion>
            <v1:datosPersonales/>
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
         <v1:codigoSesion>{session_code}</v1:codigoSesion>
         <por:MSISDN>{msisdn}</por:MSISDN>
      </por:peticionConsultarProcesosPortabilidadMovil>
   </soapenv:Body>
  </soapenv:Envelope>
    """