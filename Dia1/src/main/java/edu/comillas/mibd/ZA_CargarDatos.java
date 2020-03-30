package edu.comillas.mibd;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.*;

import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.util.Bytes;



import java.io.IOException;

import java.util.ArrayList;

import java.util.List;

public class ZA_CargarDatos {


    public static void main(String[] args){

        //Especificación de la configuración de HBase

        Configuration conf = HBaseConfiguration.create();
        String prePathDocker= "/home/icai/tmp/";
        String prePathCloudera= "/home/icai/tmp/Cloudera/";
        conf.addResource(new Path(prePathDocker+"hbase-site.xml"));
        conf.addResource(new Path(prePathDocker+"core-site.xml"));


        //Preparación de la conexión a HBase
        Connection connection=null;
        Admin adm = null;


        //TODO Modificar con un identificador del alumno
        String namespaceACrear = "jhe";

        //Crea un descripctor para el namespace
        NamespaceDescriptor namespaceDesc = NamespaceDescriptor.create(namespaceACrear).build();


        try {

            //Conectarse a la base de datos
            connection = ConnectionFactory.createConnection(conf);

            //Obtener un objeto administrador
            adm = connection.getAdmin();

            //Crear el namespace
            adm.createNamespace(namespaceDesc);


            //Crear tabla--------------------------------------
            String soloTableName = "Ejemplo1";
            String tableNameString = namespaceACrear + ":" + soloTableName;
            TableName tableName = TableName.valueOf(tableNameString);

            //Creamos el descriptor
            HTableDescriptor descTable1 = new HTableDescriptor(tableName);

            //Creamos la column family dv
            HColumnDescriptor coldef1 = new HColumnDescriptor("dv");
            //Versionado a 2
            coldef1.setMaxVersions(2);
            //Se añade la definicón de la CF1 a la definición de la tabla
            descTable1.addFamily(coldef1);

            //Crear la defnición de la CF2 'datos personales' y llamarla 'dp'
            HColumnDescriptor coldef2 = new HColumnDescriptor("dp");
            //TDefinir el versionado de la CF2 a tres
            coldef2.setMaxVersions(3);
            //Añadir la definicón de la CF2 a la definición de la tabla
            descTable1.addFamily(coldef2);

            //HASTA AQUI NO HE CREADO NADA, SOLO HE HECHO LA DESCRIPCION DE LA TABLA
            //Se crea la tabla
            adm.createTable(descTable1);

            //Si la tabla no existe se termina la ejecución
            if(!adm.tableExists(tableName)){
                System.exit(1);
            }


            //Se recupera la tabla
            Table tbl = connection.getTable(tableName);

            //Se crea una lista de objetos Put
            List<Put> puts = new ArrayList<Put>();

            //Se definen las variables que se utilizarán para dar de alta las columnas
            String rowKey;
            String fam1;
            String qual1;
            String val1;


            // Añadir cinco versiones de color a la fila 0001
            {
                rowKey = "0001";

                Put put1 = new Put(Bytes.toBytes(rowKey));

                fam1 = "dv";
                qual1 = "Color";
                val1 = "Verde";

                put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 10, Bytes.toBytes(val1));
                puts.add(put1);


                Put put2 = new Put(Bytes.toBytes(rowKey));

                fam1 = "dv";
                qual1 = "Color";
                val1 = "Violeta";

                put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 20, Bytes.toBytes(val1));
                puts.add(put2);


                Put put3 = new Put(Bytes.toBytes(rowKey));

                fam1 = "dv";
                qual1 = "Color";
                val1 = "Morado";

                put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 30, Bytes.toBytes(val1));
                puts.add(put3);


                Put put4 = new Put(Bytes.toBytes(rowKey));

                fam1 = "dv";
                qual1 = "Color";
                val1 = "Morado";

                put4.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 40, Bytes.toBytes(val1));
                puts.add(put4);


                Put put5 = new Put(Bytes.toBytes(rowKey));

                fam1 = "dv";
                qual1 = "Color";
                val1 = "Amarillo";

                put5.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 50, Bytes.toBytes(val1));
                puts.add(put5);

            }





            //Añadir datos hasta la fila 0012

            {

                rowKey = "0008";

                Put put11 = new Put(Bytes.toBytes(rowKey));

                fam1 = "dv";
                qual1 = "Color";
                val1 = "Night Blue";
                put11.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

                qual1 = "Modelo";
                val1 = "Ferrari";
                put11.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

                qual1 = "Matricula";
                val1 = "0111-ABC";
                put11.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));



                fam1 = "dp";
                qual1 = "Nombre";
                val1 = "Emilio Botin";
                put11.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

                qual1 = "Email";
                val1 = "ebotin@hotmail.com";
                put11.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));


                puts.add(put11);


                rowKey = "0011";

                Put put12 = new Put(Bytes.toBytes(rowKey));

                fam1 = "dv";
                qual1 = "Color";
                val1 = "Manhatan Grey";
                put12.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

                qual1 = "Matricula";
                val1 = "0134-HHH";
                put12.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

                fam1 = "dp";
                qual1 = "Nombre";
                val1 = "Arturo Perez";
                put12.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

                qual1 = "Email";
                val1 = "aperez@gmail.com";
                put12.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));


                puts.add(put12);




            }



//Añadir matriculas para filas entre 0100 y 0200
            {
                rowKey = "0100";

                Put put21 = new Put(Bytes.toBytes(rowKey));

                fam1 = "dv";
                qual1 = "Matricula";
                val1 = "5826-ÑÑÑ";
                put21.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

                qual1 = "Motor";
                val1 = "Hibrido";
                put21.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

                puts.add(put21);


                rowKey = "0150";

                Put put22 = new Put(Bytes.toBytes(rowKey));

                fam1 = "dv";
                qual1 = "Matricula";
                val1 = "0101-IBI";
                put22.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

                qual1 = "Motor";
                val1 = "Hibrido";
                put22.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

                puts.add(put22);


            }


//Añadir motor "oli" color "gris"

            {

                rowKey = "0200";

                Put put31 = new Put(Bytes.toBytes(rowKey));

                fam1 = "dv";
                qual1 = "Color";
                val1 = "Gris";
                put31.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 5, Bytes.toBytes(val1));

                qual1 = "Motor";
                val1 = "Gasolina";
                put31.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 5, Bytes.toBytes(val1));

                puts.add(put31);


                rowKey = "0201";

                Put put32 = new Put(Bytes.toBytes(rowKey));

                fam1 = "dv";
                qual1 = "Color";
                val1 = "Gris";
                put32.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 5, Bytes.toBytes(val1));

                qual1 = "Motor";
                val1 = "Diesel";
                put32.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 5, Bytes.toBytes(val1));

                puts.add(put32);


            }



            tbl.put(puts);

            tbl.close();



        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }


        System.out.println("FIN");

    }

}