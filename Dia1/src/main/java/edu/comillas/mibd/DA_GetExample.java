package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class DA_GetExample {
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

            //se obtiene toda la fila
            rowKey = "0001";
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = tbl.get(get);
            Visualizador.PrintResult(result);

            //Se obtienen datos concretos de un resultado
            byte[] nombre = result.getValue(Bytes.toBytes("dp"), Bytes.toBytes("Nombre"));
            byte[] modelo = result.getValue(Bytes.toBytes("dv"), Bytes.toBytes("Modelo"));
            System.out.println("Nombre: "+Bytes.toString(nombre) + ", modelo:"+ Bytes.toString(modelo));

            //se obtiene sólo el color y el modelo
            rowKey = "0002";
            get = new Get(Bytes.toBytes(rowKey));
            String fam = "dv";
            String qual = "Color";
            get.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual));
            qual = "Modelo";
            get.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual));

            result = tbl.get(get);
            Visualizador.PrintResult(result);

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
