package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class AA_ScanExample {
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



            //Se Obtiene la última versión de todas las filas
            //Se crea un objeto scan
            Scan scan = new Scan();
            //Se traen los registros
            ResultScanner scanner = tbl.getScanner(scan);

            //Se muestra el resultado
            Visualizador.PrintResult(scanner);

            //Se cierra el resultado
            scanner.close();


            //Obtener las últimas 5 versiones de todas las filas
            //Se crea un objeto scan
            scan = new Scan();
            //Se fija el número máximo de versiones a devolver
            scan.setMaxVersions(5);
            //scan.setRaw(true);
            //obtener los resultados
            scanner = tbl.getScanner(scan);

            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();



            //Obtener la última version  de la familia dp de todas las filas
            scan = new Scan();
            scan.addFamily(Bytes.toBytes("dp"));
            scanner = tbl.getScanner(scan);

            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();


            //TODO crear un objeto scan
            scan = new Scan();
            //Seleccionar Matricula, Color, Anyo desde la fila 0000 hasta la fila 0012
            scan.addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Matricula")).
                    addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Color")).
                    addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Anyo")). //anyo no hemos imputeado
                    setStartRow(Bytes.toBytes("0000")).
                    setStopRow(Bytes.toBytes("0012")); //devuelve todo hasta el 12
            scanner = tbl.getScanner(scan);

            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();


            //Obtener la última version  de matricula de las filas entre 0100 y 0200
            //TODO crear un objeto scan
            scan = new Scan();
            //TODO añadir la columna matricula
            scan.addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Matricula")).
                    setStartRow(Bytes.toBytes("0100")).
                    setStopRow(Bytes.toBytes("0200"));

            ///TODO obtener el resultado
            scanner = tbl.getScanner(scan);
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();



            //Obtener la última version  de matricula de las filas entre 0020 y 0000 en orden inverso
            //TODO crear un objeto scan
            scan = new Scan();
            //TODO añadir la columna matricula
            scan.addColumn(Bytes.toBytes("dv"), Bytes.toBytes("Matricula")).
                    setStartRow(Bytes.toBytes("0020")).
                    setStopRow(Bytes.toBytes("0000")).
                    setReversed(true);

            //TODO obtener el resultado
            scanner = tbl.getScanner(scan);
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();



            //Obtener la última version comprendidas entre 10 y 99 de las todas filas
            //TODO crear un objeto scan
            scan = new Scan();
            scan.setTimeRange(10,99);
            //TODO obtener el resultado
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
