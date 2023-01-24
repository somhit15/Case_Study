import json

def getconfig(filepath):
    with open(filepath,"r") as f:
        return json.load(f)
      
             
        
