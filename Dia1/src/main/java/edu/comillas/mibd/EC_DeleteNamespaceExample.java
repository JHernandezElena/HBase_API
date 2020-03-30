package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class EC_DeleteNamespaceExample {
    public static void main(String[] args){
        //TODO Especificación de la configuración de HBase
        //Especificación de la configuración de HBase
        Configuration conf = HBaseConfiguration.create();
        //COGE LOS VALORES POR DEFECTO SI ENCUENTRA FICHEROS DE CONF EN EL PATH DEL PROGRAMA
        String prePathDocker= "/home/icai/tmp/";
        String prePathCloudera= "/home/icai/tmp/Cloudera/"; //ahi esta la configuracion del serffvidor de ICAI
        //AHORA ESTA PARA CONECTARSE AL DOCKER
        conf.addResource(new Path(prePathDocker+"hbase-site.xml"));
        conf.addResource(new Path(prePathDocker+"core-site.xml"));

        //TODO Preparación de la conexión a HBase
        Connection connection=null;
        Admin adm = null;


        try {
            //TODO Conectarse a la base de datos
            connection = ConnectionFactory.createConnection(conf);

            //TODO Obtener un objeto administrador
            adm = connection.getAdmin();

            //TODO Modificar con el nombre definido en el ejercicio AA
            String namespace = "jhe";

            //Se obtienen todas las tablas del namespace que se quiere eliminar
            TableName[] tbls = adm.listTableNamesByNamespace(namespace);
            //Para poder borra un namespace debe estar vacio
            for (TableName tbl : tbls) {
                //TODO borra la tabla
                if (adm.isTableEnabled(tbl)){
                    //Se deshabilita la tabla que se quiere borrar
                    adm.disableTable(tbl);
                }
                //Se borra la tabla
                adm.deleteTable(tbl);

            }
            //Se borra el namespace
            adm.deleteNamespace(namespace);

            //TODO Mostrar el listado de namespaces definidos y comprobar si se ha borrado
            NamespaceDescriptor[] list = adm.listNamespaceDescriptors();
            for (NamespaceDescriptor nd : list) {
                System.out.println("List Namespace: " + nd);
            }


            //TODO Cerrar la conexión con HBase
            connection.close();



        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");

    }
}
