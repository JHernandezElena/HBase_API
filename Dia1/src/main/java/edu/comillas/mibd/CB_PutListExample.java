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

public class CB_PutListExample {
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

            //TODO crear un registro con rowKey "0002" para un vehículo de color "Gris", Modelo "Mercedes", Matricula "1234-AAA", Motor "Diesel" y como datos personales Nombre "Manolita Lopez"
            //Se define la rowKey
            rowKey = "0002";
            Put put1 = new Put(Bytes.toBytes(rowKey));

            fam1 = "dv";
            qual1 = "Color";
            val1 = "Gris";
            //Se añaden las propiedades al objeto Put
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            qual1 = "Modelo";
            val1 = "Mercedes";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            qual1 = "Matricula";
            val1 = "1234-AAA";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            qual1 = "Motor";
            val1 = "Diesel";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            fam1 = "dp";
            qual1 = "Nombre";
            val1 = "Manolita Lopez";
            //Se crea un objeto Put con la rowKey
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            //TODO añadir el registro creado a la lista puts
            puts.add(put1);


            //TODO crear un registro con rowKey "0003" para un vehículo de color "Amarillo", modelo "Volvo", matricula "1111-ABA", motor "Diesel" y como datos personales nombre "Lupita Hernandez"
            rowKey = "0003";
            Put put2 = new Put(Bytes.toBytes(rowKey));

            fam1 = "dv";
            qual1 = "Color";
            val1 = "Amarillo";
            //Se añaden las propiedades al objeto Put
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            qual1 = "Modelo";
            val1 = "Volvo";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            qual1 = "Matricula";
            val1 = "1111-ABA";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            qual1 = "Motor";
            val1 = "Diesel";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            fam1 = "dp";
            qual1 = "Nombre";
            val1 = "Lupita Hernandez";
            //Se crea un objeto Put con la rowKey
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            //TODO añadir el registro creado a la lista puts
            puts.add(put2);

            //TODO crear un registro con rowKey "0004" para un vehículo de color "Rojo", modelo "Renault", matricula "1122-ABC", motor "Gasolina" y como datos personales nombre "Felipe Pino"
            rowKey = "0004";
            Put put3 = new Put(Bytes.toBytes(rowKey));

            fam1 = "dv";
            qual1 = "Color";
            val1 = "Rojo";
            //Se añaden las propiedades al objeto Put
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            qual1 = "Modelo";
            val1 = "Reanult";
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            qual1 = "Matricula";
            val1 = "1122-ABC";
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            qual1 = "Motor";
            val1 = "Gasolina";
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            fam1 = "dp";
            qual1 = "Nombre";
            val1 = "Felipe Pino";
            //Se crea un objeto Put con la rowKey
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));
            //TODO añadir el registro creado a la lista puts
            puts.add(put3);

            //Se inserta la lista de registros en la tabla
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
