package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;


import java.io.IOException;

public class BA_CreateTable {

    public static void main(String[] args) {

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

            //TODO Definir el nombre de la tabla que se quiere crear. Poner 'Ejemplo1'
            String soloTableName = "Ejemplo1";



            String tableNameString = namespace + ":" + soloTableName;

            //NO SE LE PUEDE PASASR UN STRING A HBASE, HAY QUE PASARLE UN OBJETO
            TableName tableName1 = TableName.valueOf(tableNameString);

            //Otra forma de hacer lo mismo
            TableName tableName2 = TableName.valueOf(namespace, soloTableName);

            //Se crea un descriptor de la tabla donde se pondrá la definición de la tabla y propiedades de la tabla
            HTableDescriptor descTable1 = new HTableDescriptor(tableName1);
            //descTable1.setDurability(Durability.SKIP_WAL); ESTO DESACTIVARIA EL WAL (para que se guare todo en disco a la vez)

            //Se crea la definición de la CF1 'datos del vehículo'
            HColumnDescriptor coldef1 = new HColumnDescriptor("dv");
            //coldef1. PONER ESTO PARA VER TODAS LAS OPCIONES QUE TENEMOS
            
            //se define el versionado de de la CF1 a dos
            coldef1.setMaxVersions(2);
            //Se añade la definicón de la CF1 a la definición de la tabla
            descTable1.addFamily(coldef1);

            //TODO Crear la definición de la CF2 'datos personales' y llamarla 'dp'
            HColumnDescriptor coldef2 = new HColumnDescriptor("dp");
            //TODO Definir el versionado de la CF2 a tres
            coldef2.setMaxVersions(3);
            //TODO Añadir la definicón de la CF2 a la definición de la tabla
            descTable1.addFamily(coldef2);

            //HASTA AQUI NO HE CREADO NADA, SOLO HE HECHO LA DESCRIPCION DE LA TABLA
            //Se crea la tabla
            adm.createTable(descTable1);

            //TODO Cerrar la conexión con HBase
            connection.close();


        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("FIN");
    }
}
