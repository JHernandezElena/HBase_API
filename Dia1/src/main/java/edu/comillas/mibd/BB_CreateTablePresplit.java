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
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BB_CreateTablePresplit {
    public static void main(String[] args)
    {
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

            //TODO Definir el nombre de la tabla que se quiere crear. Poner 'Ejemplo2'
            String soloTableName = "Ejemplo2";


            String tableNameString = namespace + ":" + soloTableName;

            //NO SE LE PUEDE PASASR UN STRING A HBASE, HAY QUE PASARLE UN OBJETO
            TableName tableName1 = TableName.valueOf(tableNameString);


            //Se crea un descriptor de la tabla donde se pondrá la definición de la tabla y propiedades de la tabla
            HTableDescriptor descTable1 = new HTableDescriptor(tableName1);;
                //SE INICIALIZA A NULO PARA LUEGO DEFINIRLO

            //Se crea la definición de la CF1 'datos personales'
            HColumnDescriptor coldef1 = new HColumnDescriptor("dp");

            //se define el versionado de de la CF1 a tres
            coldef1.setMaxVersions(3);
            //Se añade la definicón de la CF1 a la definición de la tabla
            descTable1.addFamily(coldef1);

            //CREAMOS FRONTERAS PARA LAS REGIONES
            byte[][] regions = new byte[][] {
               Bytes.toBytes("0002"), //intervalo [0000, 0002),[0002, 0005) [0005, 0007), [0007, ...)
               Bytes.toBytes("0005"),
               Bytes.toBytes("0007")
            };

            //Se crea la tabla
            adm.createTable(descTable1,regions);




            //TODO Definir el nombre de la tabla que se quiere crear. Poner 'Ejemplo3'
            String soloTableName2 = "Ejemplo3";

            String tableNameString2 = namespace + ":" + soloTableName2;

            //NO SE LE PUEDE PASASR UN STRING A HBASE, HAY QUE PASARLE UN OBJETO
            TableName tableName2 = TableName.valueOf(tableNameString2);


            //TODO Crea un descriptor de la tabla donde se pondrá la definición de la tabla y propiedades de la tabla
            HTableDescriptor descTable2 = new HTableDescriptor(tableName2);

            //TODO Crear la definición de la CF1 'datos vehículo' y llamarla 'dv'
            HColumnDescriptor coldef21 = new HColumnDescriptor("dv");
            //TODO Definir el versionado de la CF1 a tres
            coldef21.setMaxVersions(3);
            //TODO Añadir la definicón de la CF1 a la definición de la tabla
            descTable2.addFamily(coldef21);

            //Se crea la tabla
            adm.createTable(descTable2,Bytes.toBytes("0000"), Bytes.toBytes("0020"), 5);


            //TODO Cerrar la conexión con HBase
            connection.close();



        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("FIN");
    }

}
