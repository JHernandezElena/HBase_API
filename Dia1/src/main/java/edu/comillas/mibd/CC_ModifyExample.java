package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CC_ModifyExample {
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

            //TODO Definir el nombre de la tabla que se quiere utilizar. Poner 'Ejemplo1'
            String soloTableName = "Ejemplo1";

            String tableNameString = namespace + ":" + soloTableName;

            TableName tableName = TableName.valueOf(tableNameString );

            //COMPROBAMOS SI LA TABLA EXISTE
            //Si la tabla no existe se termina la ejecución
            if(!adm.tableExists(tableName)){
                System.exit(1);
            }

            //TODO recuperar la tabla
            //Se recupera la tabla
            Table tbl = connection.getTable(tableName);


            //Se crea una lista de objetos Put
            List<Put> puts = new ArrayList<Put>();

            //Se definen las variables que se utilizarán para dar de alta las columnas
            String rowKey;
            String fam1;
            String qual1;
            String val1;



            //TODO crear un registro con rowKey "0002" para un vehículo de color "Verde", cc "2.0", como datos personales Direccion "Castellana 125". Poner versión del registro igual a 2
            //Se define la rowKey
            rowKey = "0002";
            Put put1 = new Put(Bytes.toBytes(rowKey));

            fam1 = "dv";
            qual1 = "Color";
            val1 = "Verde";
            //Se añaden las propiedades al objeto Put
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 2, Bytes.toBytes(val1));

            qual1 = "cc";
            val1 = "2.0";
            //Se añaden las propiedades al objeto Put
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 2, Bytes.toBytes(val1));

            fam1 = "dp";
            qual1 = "Direccion";
            val1 = "Castellana 125";
            //Se crea un objeto Put con la rowKey
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 2, Bytes.toBytes(val1));

            //TODO anadir el registro a la lista
            puts.add(put1);

            //TODO crear un registro con rowKey "0003" para un vehículo de color "Rojo", modelo "Citroen". Poner versión del registro igual a 3
            rowKey = "0003";
            Put put2 = new Put(Bytes.toBytes(rowKey));
            fam1 = "dv";
            qual1 = "Color";
            val1 = "Rojo";
            //Se añaden las propiedades al objeto Put
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 3, Bytes.toBytes(val1));

            qual1 = "Modelo";
            val1 = "Citroen";
            //Se añaden las propiedades al objeto Put
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 3, Bytes.toBytes(val1));


            //TODO anadir el registro a la lista
            puts.add(put2);

            //TODO insertar los registros en la tabla
            tbl.put(puts);

            //TODO Liberar el objeto tabla
            tbl.close();

            //TODO Cerrar la conexión con HBase
            connection.close();

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
