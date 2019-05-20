from subprocess import Popen, PIPE, STDOUT

def jarWrapper(*args):
    p = Popen(['java', '-jar']+list(args), stdout=PIPE, stderr=STDOUT)
    for line in p.stdout:
        print line

def give_jar_file_and_arg(a1):
    j = 'emotion_detectv2.jar'
    args = [j, a1] # Any number of args to be passed to the jar file
    jarWrapper(*args)


if __name__ == "__main__":
    a1 = "RT @Beaking_News: Police say 17 people now confirmed dead in #GrenfellTower fire, but warn there will be more. 17 people critical in hospital"
    give_jar_file_and_arg(a1)

