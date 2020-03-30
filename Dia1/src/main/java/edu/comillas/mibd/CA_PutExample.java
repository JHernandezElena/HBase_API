package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CA_PutExample {
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

            //Se recupera la tabla
            Table tbl = connection.getTable(tableName);

            //Se definen las variables que se utilizarán para dar de alta las columnas
            String rowKey;
            String fam1;
            String qual1;
            String val1;

            //Se define la rowKey
            rowKey = "0001";
            //Se crea un objeto Put con la rowKey
            Put put1 = new Put(Bytes.toBytes(rowKey)); //CREAMOS UN OBJETO PUT QUE SIEMPRE RECIBE EL ROWKEY EN FORMATO BYTES

            //Se define la columna 'Color'
            fam1 = "dv";
            qual1 = "Color";
            val1 = "rojo";
            //Se añaden las propiedades al objeto Put
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            //Se define la columna 'Modelo'
            qual1 = "Modelo";
            val1 = "Seat";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            //TODO Definir la columna 'Matricula' con valor '0000-AAA'
            qual1 = "Matricula";
            val1 = "0000-AAA";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));


            //TODO Definir la columna 'Motor' con valor 'Diesel'
            qual1 = "Motor";
            val1 = "Diesel";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));


            //TODO Definir la columna 'Nombre' de la CF='dp' con valor 'Juanito Perez'
            fam1 = "dp";
            qual1 = "Nombre";
            val1 = "Juanito Perez";
            //Se añaden las propiedades al objeto Put
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));




            //Se inserta el registro en la tabla
            tbl.put(put1);

            //Se libera el objeto tabla
            tbl.close();

            //TODO Cerrar la conexión con HBase
            connection.close();



        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("FIN");
    }
}
