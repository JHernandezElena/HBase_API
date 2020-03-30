//COMO SE HARIA CON EL API NUEVO
//PARA LA VERSIO 2 DE HBASE

package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BAA_CreateTable {

    public static void main(String[] args) {
/*
        //TODO Especificación de la configuración de HBase

        //TODO Preparación de la conexión a HBase

        Admin adm = null;

        try {
            //TODO Conectarse a la base de datos


            //TODO Obtener un objeto administrador



            //TODO Modificar con el nombre definido en el ejercicio AA
            String namespace = "???";

            //TODO Definir el nombre de la tabla que se quiere crear. Poner 'Ejemplo4'
            String soloTableName = "";


            String tableNameString = namespace + ":" + soloTableName;

            TableName tableName1 = TableName.valueOf(tableNameString);

            //Otra forma de hacer lo mismo
            TableName tableName2 = TableName.valueOf(namespace, soloTableName);

            //Se crea el objeto con la definición de la tabla
            TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tableName1);
//            tableDescBuilder.setDurability(Durability.SKIP_WAL);

            //Se crea el objeto con la definición de la CF dv
            ColumnFamilyDescriptorBuilder columnDescBuilder1 = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes("dv"))
                    .setMaxVersions(2);
//                    .setBlocksize(32 * 1024)
//                    .setCompressionType(Compression.Algorithm.SNAPPY)
//                    .setDataBlockEncoding(DataBlockEncoding.NONE);

            //Se añade la descripción de la CF dv a la descripción de la tabla
            tableDescBuilder.setColumnFamily(columnDescBuilder1.build());

            //Se crea el objeto con la definición de la CF dp
            ColumnFamilyDescriptorBuilder columnDescBuilder2 = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes("dp"))
                    .setMaxVersions(3);
//                    .setBlocksize(32 * 1024)
//                    .setCompressionType(Compression.Algorithm.SNAPPY)
//                    .setDataBlockEncoding(DataBlockEncoding.NONE);
            //Se añade la descripción de la CF dp a la descripción de la tabla
            tableDescBuilder.setColumnFamily(columnDescBuilder2.build());

            //Se crea la tabla
            adm.createTable(tableDescBuilder.build());



            //TODO Cerrar la conexión con HBase


        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("FIN");

 */
    }
}
