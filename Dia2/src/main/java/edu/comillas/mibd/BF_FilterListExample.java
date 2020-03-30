package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BF_FilterListExample {
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



            //Se crea un filtro
            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("dv"), Bytes.toBytes("Motor"),
                    CompareFilter.CompareOp.NOT_EQUAL,
                    new SubstringComparator("oli"));
            filter1.setFilterIfMissing(true);

            //Se crea un filtro
            SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("dv"), Bytes.toBytes("Color"),
                    CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator("Gris"));
            filter2.setFilterIfMissing(true);

            //Se crea una estructura de lista de filtros
            List<Filter> filters = new ArrayList<Filter>();

            //Se añaden los filtro a la lista
            filters.add(filter1);
            filters.add(filter2);

            //Se crea un Filtro Lista donde se tienen que cumplir todos los filtros. (AND)
            FilterList filterList1 = new FilterList(filters);


            //TODO Crear un objeto Scan
            Scan scan = new Scan();
            //TODO Asociar el Filtro Lista al objeto scan
            scan.setFilter(filterList1);
            ///TODO obtener el resultado
            ResultScanner scanner = tbl.getScanner(scan); //LA PRIMERA VEZ QUE OPBTENEMOS EL RESUTADO HAY QUE CREARLO, LUEGO NO HAY QUE PONER LO DE RESULT SCANNER
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();

            //Se crea un Filtro Lista donde se tienen que cumplir alguno de los filtros. (OR)
            FilterList filterList2 = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
            //TODO Asociar el filtro al objeto scan
            scan.setFilter(filterList2);
            //TODO Obtener el resultado
            scanner = tbl.getScanner(scan); //LA PRIMERA VEZ QUE OPBTENEMOS EL RESUTADO HAY QUE CREARLO, LUEGO NO HAY QUE PONER LO DE RESULT SCANNER
            //TODO Mostrar el resltado
            Visualizador.PrintResult(scanner);
            //TODO Cerrar el resultado
            scanner.close();


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
