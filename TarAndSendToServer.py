import subprocess

sevenZip = 'C:\\Program Files (x86)\\7-Zip\\7z.exe'

stagingDir = 'C:\\Users\\carterj3\\Desktop\\Hadoop\\Showdown\\Staging\\'
playDir = 'C:\\Users\\carterj3\\Desktop\\Hadoop\\Showdown\\Log\\'

hdfsLoc = 'dhcp-137-112-147-135.reshall.rose-hulman.edu:8020'
hdfsJar = 'C:\\Users\\carterj3\\Desktop\\Hadoop\\Showdown\\HadoopPut-0.0.1-SNAPSHOT.jar'

def moveFile(localLocation,hdfsLocation):
  print('java', '-cp', hdfsJar, 'ScriptMain', hdfsLoc, localLocation, hdfsLocation)
  subprocess.call(['java', '-jar', hdfsJar,  hdfsLoc, localLocation, hdfsLocation], stdin=subprocess.PIPE)
  
def tarFile(folderName):
  print(sevenZip, 'a', '-ttar', stagingDir+'\\'+folderName+'.tar', playDir+folderName+'\\*')
  subprocess.call([sevenZip, 'a', '-ttar', stagingDir+'\\'+folderName+'.tar', playDir+folderName+'\\*'], stdin=subprocess.PIPE)

def getDay():
  return "2014-10-30"

if __name__ == "__main__"
    tarFile(getDay())
    moveFile(stagingDir+getDay()+'.tar','/tmp/Showdown/Exp/'+getDay()+'.tar')