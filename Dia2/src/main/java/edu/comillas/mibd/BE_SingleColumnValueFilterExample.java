package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BE_SingleColumnValueFilterExample {
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


            //TODO Crear un objeto Scan
            Scan scan = new Scan();
            //se define un filtro donde el valor NO contenga "oli, pero sólo en la columna "Motor""
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("dv"),
                    Bytes.toBytes("Motor"),
                    CompareFilter.CompareOp.NOT_EQUAL,
                    new SubstringComparator("oli")); //DEVUELVE LO QUE NO SEA GASOLINA (NOT EQUAL)
            //Definir el comportamiento del filtro cuando la columna no exista
            //El valor por defecto es false => row pasa si no se encuentra la columna
            filter.setFilterIfMissing(true);
            //TODO Asociar el filtro al objeto scan
            scan.setFilter(filter);
            ///TODO obtener el resultado
            ResultScanner scanner = tbl.getScanner(scan); //LA PRIMERA VEZ QUE OPBTENEMOS EL RESUTADO HAY QUE CREARLO, LUEGO NO HAY QUE PONER LO DE RESULT SCANNER
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();


            System.out.println("\n\n");



            //TODO Analizar el resultado del siguiente codigo
            scan = new Scan();
            scan.addColumn(Bytes.toBytes("dv"),Bytes.toBytes("Matricula"));
            //TODO Asociar el filtro al objeto scan
            scan.setFilter(filter);
            ///TODO obtener el resultado
            scanner = tbl.getScanner(scan);
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();
                //NO DEVUELVE NADA PORQUE SOLO SCANEAMOS MATRICULA Y ESTAMOS FILTRANDO POR MOTOR


            //TODO Liberar el objeto tabla
            tbl.close();
            //TODO Cerrar la conexión con HBase
            connection.close();

            //TODO borrar al terminar el ejercicio
            throw new IOException();

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }

}
