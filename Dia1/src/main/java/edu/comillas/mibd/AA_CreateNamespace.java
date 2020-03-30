package edu.comillas.mibd;


//IMPORTAMOS LAS LIBRERIAS Y DEPENDENCIAS QUE NECESITAMOS
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


public class AA_CreateNamespace {
    public static void main(String[] args) {
        //Especificación de la configuración de HBase
        Configuration conf = HBaseConfiguration.create();
                //COGE LOS VALORES POR DEFECTO SI ENCUENTRA FICHEROS DE CONF EN EL PATH DEL PROGRAMA
        String prePathDocker= "/home/icai/tmp/";
        String prePathCloudera= "/home/icai/tmp/Cloudera/"; //ahi esta la configuracion del servidor de ICAI
        //AHORA ESTA PARA CONECTARSE AL CLUSTER
        conf.addResource(new Path(prePathDocker+"hbase-site.xml"));
        conf.addResource(new Path(prePathDocker+"core-site.xml"));

        //Preparación de la conexión a HBase
        Connection connection=null;
        Admin adm = null;

        //TODO Modificar con un identificador del alumno
        //tiene que no estar creado anteriormente
        String namespaceACrear = "jhe";

        //Crea un descripctor para el namespace
        NamespaceDescriptor namespaceDesc = NamespaceDescriptor.create(namespaceACrear).build();
        try {
            //Conectarse a la base de datos
            connection = ConnectionFactory.createConnection(conf);

            //Obtener un objeto administrador
            adm = connection.getAdmin();

            //Crear el namespace
            adm.createNamespace(namespaceDesc);

            //Mostrar todos los namespace creados
            NamespaceDescriptor[] list = adm.listNamespaceDescriptors();
            for (NamespaceDescriptor nd : list) {
                System.out.println("List Namespace: " + nd);
            }

            //Se cierra la conexión con HBase
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("FIN");

    }
}
