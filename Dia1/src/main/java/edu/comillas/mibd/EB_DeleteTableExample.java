package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;


import java.io.IOException;

public class EB_DeleteTableExample {
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

            //TODO Definir el nombre de la tabla que se quiere utilizar. Poner 'Ejemplo2'
            String soloTableName = "Ejemplo2";

            String tableNameString = namespace + ":" + soloTableName;

            TableName table = TableName.valueOf(tableNameString );

            //COMPROBAMOS SI LA TABLA EXISTE
            //Si la tabla no existe se termina la ejecución
            if(!adm.tableExists(table)){
                System.exit(1);
            }
            //Para poder borrar una tabla tiene que estar deshabilitada
            if (adm.isTableEnabled(table)){
                //Se deshabilita la tabla que se quiere borrar
                adm.disableTable(table);
            }
            //Se borra la tabla
            adm.deleteTable(table);


        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");

    }

}
