import subprocess

sevenZip = 'C:\\Program Files (x86)\\7-Zip\\7z.exe'

hdfsLoc = 'dhcp-137-112-147-135.reshall.rose-hulman.edu:8020'
hdfsJar = 'C:\\Users\\carterj3\\Desktop\\Hadoop\\Showdown\\HadoopPut-0.0.1-SNAPSHOT.jar'

# python C:\Users\carterj3\git\HadoopShowdown\WS_MASTER.py
# hadoop fs -ls /tmp/Showdown/Data
# hadoop fs -rm  /tmp/Showdown/Data/2014-10-30.tar


def moveFile(localLocation,hdfsLocation):
  # Using Popen stops this from blocking I think.
  print('java', '-jar', hdfsJar,  hdfsLoc, localLocation, hdfsLocation)
  subprocess.Popen(['java', '-jar', hdfsJar,  hdfsLoc, localLocation, hdfsLocation], stdin=subprocess.PIPE)
  
def tarFile(folderName,fileName):
  # print(sevenZip, 'a', '-ttar', stagingDir+'\\'+folderName+'.tar', playDir+folderName+'\\*')
  subprocess.call([sevenZip, 'a', '-ttar', fileName, folderName+'\\*'], stdin=subprocess.PIPE)

def getDay():
  return "2014-10-30"

if __name__ == "__main__":
    tarFile(getDay())
    moveFile(stagingDir+getDay()+'.tar','/tmp/Showdown/Exp/'+getDay()+'.tar')