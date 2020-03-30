package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

public class CA_OtrosFiltrosExample {
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

            //TODO Conectarse a la tabla 'Ejemplo1' de cada alumno
            String namespace = "jhe";
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


    //TODO Realizar otras consultas sobre la tabla 'Ejemplo1' de cada alumno utilizando otros filtros y comparadores no utilizados en los ejemplos
    //TODO Para ello consultar la documentación en http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/package-summary.html

            //Se crea un objeto scan
            Scan scan = new Scan();
            //Simple filter that returns first N columns on row only.
            Filter filter = new ColumnCountGetFilter(2);
            //Se asocia el filtro al objeto scan
            scan.setFilter(filter);
            ///TODO obtener el resultado
            ResultScanner scanner = tbl.getScanner(scan); //LA PRIMERA VEZ QUE OPBTENEMOS EL RESUTADO HAY QUE CREARLO, LUEGO NO HAY QUE PONER LO DE RESULT SCANNER
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();

            System.out.println("\n\n");

            //Se crea un objeto scan
            scan = new Scan();
            //Filter that returns solely the primary key-value from every row
            filter = new  FirstKeyOnlyFilter();
            //Se asocia el filtro al objeto scan
            scan.setFilter(filter);
            ///TODO obtener el resultado
            scanner = tbl.getScanner(scan);
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();

            System.out.println("\n\n");

            //Se crea un objeto scan
            scan = new Scan();
            //Filter that returns solely the key part of every key-value
            filter = new  KeyOnlyFilter ();
            //Se asocia el filtro al objeto scan
            scan.setFilter(filter);
            ///TODO obtener el resultado
            scanner = tbl.getScanner(scan);
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();

            System.out.println("\n\n");

            //Se crea un objeto scan
            scan = new Scan();
            //Filter that returns solely those key-values present in the very column that starts with the specified column prefix
            filter = new  ColumnPrefixFilter(Bytes.toBytes("Nombre"));
            //Se asocia el filtro al objeto scan
            scan.setFilter(filter);
            ///TODO obtener el resultado
            scanner = tbl.getScanner(scan);
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();

            System.out.println("\n\n");

            //Se crea un objeto scan
            scan = new Scan();
            //Filter that returns the primary limit number of columns within the table.
            filter = new  ColumnCountGetFilter(1);
            //Se asocia el filtro al objeto scan
            scan.setFilter(filter);
            ///TODO obtener el resultado
            scanner = tbl.getScanner(scan);
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();

            System.out.println("\n\n");

            //Se crea un objeto scan
            scan = new Scan();
            //Filter that returns  all key-values present in rows up to the specified row (included)
            filter = new  InclusiveStopFilter(Bytes.toBytes("0003"));
            //Se asocia el filtro al objeto scan
            scan.setFilter(filter);
            ///TODO obtener el resultado
            scanner = tbl.getScanner(scan);
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();

            System.out.println("\n\n");

            //Se crea un objeto scan
            scan = new Scan();
            //Filter that returns those key-values whose timestamps match any of the specified timestamps (last ts).
            List<Long> list = new ArrayList<Long>();
            list.add(50L);
            list.add(20L);
            filter = new  TimestampsFilter(list);
            //Se asocia el filtro al objeto scan
            scan.setFilter(filter);
            ///TODO obtener el resultado
            scanner = tbl.getScanner(scan);
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();





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
