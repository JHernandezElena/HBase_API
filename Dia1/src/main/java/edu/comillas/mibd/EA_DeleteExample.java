package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class EA_DeleteExample {
    public static void main(String[] args) {
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

            String rowKey = "0003";
            //Se crea un objeto delete para borrar
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            //Se borra toda la fila
            tbl.delete(delete);

            //TODO Obtener toda la fila del rowKey = "0003"
            //se obtiene toda la fila
            rowKey = "0003";
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = tbl.get(get);
            Visualizador.PrintResult(result);




            rowKey = "0002";
            //Se crea un objeto delete para borrar
            delete = new Delete(Bytes.toBytes(rowKey));
            //Se limita el borrado a la column family 'datos personales'
            delete.addFamily(Bytes.toBytes("dp"));
            //Se borra sólo la totalidad de la cf 'dp'
            tbl.delete(delete); //BORRA TODAS LAS VERSIONES
            //TODO Obtener toda la fila del rowKey = "0002"
            //se obtiene toda la fila
            rowKey = "0002";
            Get get2 = new Get(Bytes.toBytes(rowKey));
            Result result2 = tbl.get(get);
            Visualizador.PrintResult(result2);



            //rowKey = "0002";
            delete = new Delete(Bytes.toBytes(rowKey));
            //Se añade el cualificador "Color"
            delete.addColumns(Bytes.toBytes("dv"),Bytes.toBytes("Color"));
            //TODO añadir el cualificador "Matricula"
            delete.addColumns(Bytes.toBytes("dv"),Bytes.toBytes("Matricula"));
            //TODO borrar el elemento de la tabla
            tbl.delete(delete);
            //TODO Obtener toda la fila del rowKey = "0002"
            Get get3 = new Get(Bytes.toBytes(rowKey));
            Result result3 = tbl.get(get);
            Visualizador.PrintResult(result3);



            //SI YO LLAMO AL DELETE SIN TIMESTAMP Y SIN S, SOLO BORRA LA ULTIMA
            //SIN TIMESTAMP Y CON S BORRA TODO
            //CON TIMESTAMP SIN S BORRARA EXACTAMENTE ESA VERSION EXACTA, SI ESTA VACIA
            //CON TIMESTAMP CON S BORRA TODOLO LO IGUAL Y ANTERIOR A ESE TIMESTAMP
            rowKey = "0001";
            //TODO crear un objeto Delete para el rowKey
            delete = new Delete(Bytes.toBytes(rowKey));
            //OJO con la diferencia addcolumn y addColumns
            delete.addColumns(Bytes.toBytes("dv"),Bytes.toBytes("Color"),50);
            //TODO borrar el elemento de la tabla
            tbl.delete(delete);

            //TODO Obtener toda la fila del rowKey = "0001
            rowKey = "0001";
            Get get4 = new Get(Bytes.toBytes(rowKey));
            Result result4 = tbl.get(get);
            Visualizador.PrintResult(result4);



            ///TODO Liberar el objeto tabla
            tbl.close();

            //TODO Cerrar la conexión con HBase
            connection.close();



            //TODO volver a ejecutar las inserciones (CA y CB) y las funciones get (DA y DB) y comentar lo que se aprecia


        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }

}
