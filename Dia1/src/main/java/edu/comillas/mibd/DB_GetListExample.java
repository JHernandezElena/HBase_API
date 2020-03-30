package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DB_GetListExample {
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

            //TODO Conectarse a la tabla de cada alumno
            //Se recupera la tabla
            Table tbl = connection.getTable(tableName);

            //Se definen las variables que se vana utilizar para consultar la tabla
            byte[] cf1 = Bytes.toBytes("dv");
            byte[] cf2 = Bytes.toBytes("dp");
            byte[] qf1 = Bytes.toBytes("Color");
            byte[] qf2 = Bytes.toBytes("Modelo");
            byte[] qf3 = Bytes.toBytes("Matricula");
            byte[] qf4 = Bytes.toBytes("Motor");
            byte[] qf5 = Bytes.toBytes("Nombre");
            byte[] row1 = Bytes.toBytes("0001");
            byte[] row2 = Bytes.toBytes("0002");
            byte[] row3 = Bytes.toBytes("0003");

            //Se define una lista para alamacenar los objetos Get
            List<Get> gets = new ArrayList<Get>();

            //Se define el primer objeto Get que se quiere obtener
            Get get1 = new Get(row1);
            get1.addColumn(cf1, qf1);
            //Se añade el objeto get a la lista
            gets.add(get1);

            //TODO PARA 0002
            Get get2 = new Get(row2);
            //TODO limitar el resultado al nombre del propietario
            get2.addColumn(cf2, qf5);;
            //TODO Añadir el objeto a la lista
            gets.add(get2);


            //TODO Añadir a la lista un objeto para obtener la matricula del row 0003
            Get get3 = new Get(row3);
            get3.addColumn(cf1, qf3);
            gets.add(get3);


            Result[] results = tbl.get(gets);

            for (Result result : results) {
                Visualizador.PrintResult(result);
            }

            ///TODO Liberar el objeto tabla
            tbl.close();

            //TODO Cerrar la conexión con HBase
            connection.close();


        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
        System.out.println("FIN");
    }



}
