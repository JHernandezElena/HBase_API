package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BG_PageFilterExample {

    private static final byte[] POSTFIX = new byte[] { 0x00 };


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



            //se define un filtro de paginación con tamaño de pagina 3
            Filter filter = new PageFilter(3);

            //Se inicializa un contador para el numero de registros totales leidos
            int totalRows = 0;
            //Se define una variable donde almacenar el rowKey de la última fila leida
            byte[] lastRow = null;
            //Se define un bucle infinito
            while (true) {
                //Se crea un objeto Scan
                Scan scan = new Scan();
                //Se asocia el filtro de paginacion al objeto scan
                scan.setFilter(filter);
                //La primera iteración no se pasa por aquí
                if (lastRow != null) {
                    //Se define como primera fila la última de la iteración anterior
                    byte[] startRow = Bytes.add(lastRow, POSTFIX);
                    System.out.println("start row: " +
                            Bytes.toStringBinary(startRow));
                    scan.setStartRow(startRow);
                }
                //Se obtiene el resultado
                ResultScanner scanner = tbl.getScanner(scan);
                //Se define un contador para verificar el numero de filas devueltas por el filtro
                int localRows = 0;
                Result result;
                //Se itera por las filas del resultado
                while ((result = scanner.next()) != null) {
                    System.out.println(localRows++ + ": " + result);
                    //Se postincrementa el numero de registros leidos
                    totalRows++;
                    //Se almacena el rowKey del último registro leido
                    lastRow = result.getRow();
                }
                //Se cierra el scaner
                scanner.close();
                //Si el filtro no devolvio ningun registro se termina el bucle infinito
                if (localRows == 0) break;
            }
            System.out.println("total rows: " + totalRows);


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
