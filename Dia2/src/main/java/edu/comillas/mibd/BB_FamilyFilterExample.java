package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BB_FamilyFilterExample {
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




            //Se crea un objeto scan
            Scan scan = new Scan();
            //se define un filtro donde la column family sea menor que "dv"
            Filter filter = new FamilyFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("dv")));
            //Se asocia el filtro al objeto scan
            scan.setFilter(filter);
            ///TODO obtener el resultado
            ResultScanner scanner = tbl.getScanner(scan); //LA PRIMERA VEZ QUE OPBTENEMOS EL RESUTADO HAY QUE CREARLO, LUEGO NO HAY QUE PONER LO DE RESULT SCANNER
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();



            //También se pueden asociar filtros a objetos get
            Get get = new Get(Bytes.toBytes("0001")); //no devuelve nada porque la fila 1 no tiene dv (get 'jhe:Ejemplo1', "0001")
            get.setFilter(filter);
            //TODO obtener el resultado
            Result result = tbl.get(get);
            //TODO Mostrar el resltado
            Visualizador.PrintResult(result);


            //TODO Analizar el siguiente código
            {
                Filter filter2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("dv"))); //filtro que se aplica sobre el resultado
                Get get2 = new Get(Bytes.toBytes("0004"));
                get2.addFamily(Bytes.toBytes("dp")); //solo estamos cogiendo en el conjunto resultado dp
                get2.setFilter(filter2);
                Result result2 = tbl.get(get2);
                System.out.println("");
                System.out.println("Result of get(): " + result2); //por lo que esto no devolvera nada
            }

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
