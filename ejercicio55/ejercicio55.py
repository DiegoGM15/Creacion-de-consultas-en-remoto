import os
from os import system
system("cls")
import numpy as np
import mysql.connector
from datetime import date
from datetime import timedelta 
from dateutil.relativedelta import relativedelta 

conexion1=mysql.connector.connect(host="192.168.1.190", user="alumno", passwd="mipassword", db="ejercicio55")
cursor1 = conexion1.cursor()
carpeta = "c:/nueva/ejercicios_sql"

cursor1.execute("select * from clientes inner join pedidos on clientes.codigo_cli = pedidos.pedido_cod_cli where nombre = 'Manuel' or nombre = 'Antonio' order by apellidos;")

longitud = 0
tabla = list()

for fila in cursor1:

    ancho = len(fila)
    tabla.append(fila)
    longitud += 1

copia = np.array(tabla)

if(len(copia) >0):

    encabezado = np.array([["codigo_cli"],["nombre"],["apellidos"],["direccion"],["poblacion"],["codigo_postal"],["provincia"],["telefono"],["fecha_nac"]])
    completo = np.concatenate((encabezado, copia), axis=0)

    np.savetxt(carpeta + "nombres.csv", completo, delimiter=';', fmt=['%s','%s','%s','%s','%s','%s','%s','%s','%s'])

else:
    print("No hay registros con esos nombres")

today = date.today()

treintaycinco = today + relativedelta(years= -35)
veinticinco = today + relativedelta(years= -25)

valor1 = str(treintaycinco).replace("-","")
valor2 = str(veinticinco).replace("-","")

cursor1.execute("select nombre, apellidos, telefono, direccion, poblacion from clientes where poblacion = 'Ourense' and fecha_nac >= '" + valor1 + "'and fecha_nac <= '" + valor2 + "'order by fecha_nac;")

longitud = 0
tabla1 = list()

for fila in cursor1:

    ancho = len(fila)
    tabla1.append(fila)
    longitud += 1

ourensanos = np.array(tabla1)

if(len(ourensanos) >0):
    
    salida = np.array([["nombre","apellidos","telefono","direccion","poblacion"]])
    sal_ou = np.concatenate((salida, ourensanos), axis=0)

    np.savetxt(carpeta + "ourensanos.csv", sal_ou, delimiter=';', fmt=['%s','%s','%s','%s','%s'])

else:
    print("No hay registros con esos filtros")


cursor1.execute("select nombre, apellidos from clientes inner join pedidos on clientes.codigo_cli = pedidos.pedido_cod_cli where telefono = ' ' or telefono is null ;")

longitud = 0
tabla2 = list()

for fila in cursor1:

    ancho = len(fila)
    tabla2.append(fila)
    longitud += 1

sin_tlf = np.array(tabla2)

if(len(sin_tlf)>0):

    sintelsalida = np.array([["nombre","apellidos"]])
    sal_sintel = np.concatenate((sintelsalida, sin_tlf), axis=0)
    np.savetxt(carpeta + "sin_tlf.csv", sal_sintel, delimiter=';', fmt=['%s','%s'])

else:
    print("No hay registros con esos filtros")

cursor1.execute("select codigo_alm, precio, stock, reponer from almacen;")

longitud = 0
tabla3 = list()

for fila in cursor1:

    ancho = len(fila)
    tabla3.append(fila)
    longitud += 1

cuantos = np.array(tabla3)

totalarticulos = 0 
valortotal = 0
linea = ""

for i in range(cuantos.shape[0]):
    
    totalarticulos = totalarticulos + 1 #Total de articulos en el almacen
    productovt = np.prod(cuantos[i, 1:3], dtype=np.float64)
    

    if(float(cuantos[i][2]) < float(cuantos[i][3])):

        linea = linea + str(cuantos[i]) + "\t"
        
    else:

        productovt = float(cuantos[i][2]) - float(cuantos[i][3])
        valorparcial = float(cuantos[i][1]) * productovt #Valor total del almacen   261054
        valortotal = valortotal + valorparcial

totalstock = np.sum(cuantos[:, 2], dtype=np.float64) #Total stock

if(len(cuantos)>0):

    salidacuantos = np.array([["Total de articulos", totalarticulos],["articulos a reponer",linea],["Total stock", totalstock]])
    np.savetxt(carpeta + "cuantos.csv", salidacuantos, delimiter=';', fmt=['%s','%s'])
    salidavalortotal = np.array([["Valor total del almacen: ", valortotal]])
    np.savetxt(carpeta + "valor.csv", salidavalortotal, delimiter=';', fmt=['%s','%s'])

else:
    print("No hay registros con esos filtros")

cod_cliente = input("Introduce el codigo de cliente: ")
cursor1.execute("select nombre, apellidos, descripcion, pedidos.* from pedidos inner join clientes on pedidos.pedido_cod_cli = clientes.codigo_cli inner join almacen on pedidos.pedido_cod_alm = almacen.codigo_alm  where entregado = 0 and codigo_cli = '" + cod_cliente + "' ;")

longitud = 0
tabla4 = list()

for fila in cursor1:

    ancho = len(fila)
    tabla4.append(fila)
    longitud += 1

pedidos_cli = np.array(tabla4)

if(len(tabla4) > 0):

    sal_ped_cli = np.array([["nombre","apellidos","descripcion","cod_ped","numero_ped","pedido_cod_cli","pedido_cod_alm","fecha_hora","vendedor","cantidad","precio_total","entregado"]])
    salidacli = np.concatenate((sal_ped_cli, pedidos_cli), axis=0)
    np.savetxt(carpeta + "pedidos_cli.csv", pedidos_cli, delimiter=';', fmt=['%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'])

else:

    print("No hay registros con ese codigo de cliente")

conexion1.close()






















