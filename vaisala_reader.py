# -*- coding: utf-8 -*-
"""
@author: Recep Balbay
"""


from serial import Serial
from time import strftime, sleep
from os import path, mkdir
import mysql.connector as mariadb
from datetime import datetime
from sys import stdout

dataFolderPath = "/home/pi/Desktop/vDATA/"  # home path
device = "/dev/ttyUSB0"                     # USB port (COM, ttyUSB0)
vData = ["0"] * 9

startVal = 0

vSQL = "INSERT INTO `2020`(`date`, `wind_direction`, `wind_speed`, `temp_air`, `rel_humidity`, `pressure`, " \
       "`temp_heater`, `voltage_supply`, `voltage_heater`, `voltage_reference`) " \
      "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"


def UtcNow():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def DataFolder():
    year = datetime.utcnow().strftime("%Y")
    print("\n")

    if path.isdir(dataFolderPath):
        print(UtcNow() + " [INFO] DATA Folder is found.")
    else:
        try:
            print(UtcNow() + " [INFO] DATA Folder not found. Creating.")
            mkdir(dataFolderPath)
            sleep(1)
            print(UtcNow() + " [INFO] DATA Folder created: " + dataFolderPath)

        except Exception as e:
            print(UtcNow() + " [FAILURE] " + str(e.args))

    sleep(1)

    if path.isdir(dataFolderPath + year):
        print(UtcNow() + " [INFO] Year Folder is found.")
    else:
        try:
            print(UtcNow() + " [INFO] Year Folder not found. Creating.")
            mkdir(dataFolderPath + year)
            sleep(1)
            print(UtcNow() + " [INFO] Year Folder created: " + dataFolderPath + year)

        except Exception as e:
            print(UtcNow() + " [FAILURE] " + str(e.args))

    sleep(1)


def DayFile():
    year = datetime.utcnow().strftime("%Y") + "/"
    dayFile = strftime("%Y%m%d") + ".txt"
    dayFilePath = dataFolderPath + year + dayFile

    if path.isfile(dayFilePath):
        print(UtcNow() + " [INFO] DayFile is found.")
    else:
        print(UtcNow() + " [INFO] DayFile is not found. Creating...")
        try:
            with open(dayFilePath, "w") as tmp:
                tmp.write(
                    "YYYY-mm-DD HH:MM:SS, Wind Direction, Wind Speed, Air Temperature, Relative Humudity, Pressure,"
                    "Heater Temperature, Supply Voltage, Heater Voltage, Reference Voltage\n")

            print(UtcNow() + " [INFO] DayFile is created. Path: " + dayFilePath)

        except Exception as e:
            print(UtcNow() + " [FAILURE] " + str(e.args))

    sleep(1)
    return dayFilePath


with Serial(device, 19200, timeout=1) as Ser:
    while True:
        if startVal == 0:
            sleep(1)
            print("----------------------- ATASAM VAISALA PROGRAM v.2.1 -------------------------")
            print("            Program, first check and/or make folders. Please wait.            ")
            print("------------------------------------------------------------------------------")
            startVal = int(1)
            sleep(1)
        else:

            data = Ser.readlines()

            for row in data:
                VaisalaDATA = row.decode('ascii').strip().split(',')

                if len(VaisalaDATA) == 6:
                    vData[0] = VaisalaDATA[1]
                    vData[1] = VaisalaDATA[3]

                elif len(VaisalaDATA) == 13:
                    vData[2] = VaisalaDATA[2]
                    vData[3] = VaisalaDATA[6]
                    vData[4] = VaisalaDATA[10]

                elif len(VaisalaDATA) == 17:
                    vData[5] = VaisalaDATA[2]
                    vData[6] = VaisalaDATA[6]
                    vData[7] = VaisalaDATA[10]
                    vData[8] = VaisalaDATA[14]

            SN = strftime('%S')

            if SN < str(30):
                stdout.write("\r" + UtcNow() + " [INFO] Next job is starting in: " + str(30 - int(SN)) + " sn")
                stdout.flush()

            elif SN < str(33):

                DataFolder()
                dF = DayFile()

                Tarih = datetime.utcnow().strftime("%Y-%m-%d %H:%M:00")
                SqlV = (Tarih, vData[0], vData[1], vData[2], vData[3], vData[4], vData[5], vData[6], vData[7], vData[8])
                vLine = Tarih + "," + vData[0] + "," + vData[1] + "," + vData[2] + "," + vData[3] + "," + \
                        vData[4] + "," + vData[5] + "," + vData[6] + "," + vData[7] + "," + vData[8] + "\n"

                try:
                    with open(dF, "a") as tmp:
                        tmp.write(vLine)
                    print(UtcNow() + " [SUCCESS] Vaisala data is recorded.")

                except Exception as e:
                    print(UtcNow() + " [FAILURE] " + str(e.args))

                try:
                    AtaDb = mariadb.connect(
                        host="",                    # Server IP adres
                        user="",                    # Username
                        passwd="",                  # Password
                        database='vaisala-mam')

                    AtaDbCursor = AtaDb.cursor()
                    AtaDbCursor.execute(vSQL, SqlV)
                    AtaDb.commit()
                    print(UtcNow() + " [SUCCESS] Vaisala data is uploaded.")

                except Exception as sqlError:
                    print(UtcNow() + " [FAILURE] " + getattr(sqlError, 'message', repr(sqlError)))
