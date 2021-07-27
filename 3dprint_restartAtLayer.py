fileName = input("filename: \n")
layerNumber = input("layernumber: \n")
#layerNumber = "20"
#fileName = "CE3PRO_Intake_Manifold_scaled.gcode"

file = open(fileName)
layerText = ";LAYER:" + layerNumber + "\n"


#steps: find layer. delete from layer_count to desired layer exclusive
#remove lines starting with first g1 line to layer_count
#insert " X Y" on G28
#insert M106 S100 after that line
#replace G92 E0 with second G1 E value

contents = file.readlines()
file.close()


for line in contents:
    if layerText in line:
        layerIndex = contents.index(line)
    if ";LAYER:0" in line:
        layerZeroIndex = contents.index(line)
    if "Move Z Axis up little to prevent scratching of Heat Bed" in line:
        g1Index = contents.index(line)
    if ";LAYER_COUNT:" in line:
        layerCountIndex = contents.index(line)
    if "G28 ; Home all axes" in line:
        homeAxisIndex = contents.index(line)
    if "G92 E0 ; Reset Extruder" in line:
        resetExtIndex = contents.index(line)

eValue = contents[layerIndex+5].split(" ")[3]
        

del contents[layerZeroIndex:layerIndex]
del contents[g1Index:layerCountIndex]
contents[homeAxisIndex] = "G28 X Y\nM106 S100\n"
contents[resetExtIndex] = "G92 " + eValue

file = open(fileName.replace(".gcode","") + "_layer_" + layerNumber + ".gcode", "w")
file.writelines(contents)
file.close()
