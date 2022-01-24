from heapq import merge
from itertools import islice
import tempfile
from contextlib import ExitStack

#function that is loading data from file chunk by chunk, sorting those chunks and then merging them
def external_sort(filename, output):
    chunk_names = []
            
    #chunk and sort
    try:
        with open(filename) as input_file:
            while True:
                # read in next 50k lines and sort them
                sorted_chunk = (islice(input_file, 50000))
                sorted_chunk = sorted([line.replace('<', '').replace('>','') for line in sorted_chunk], key=lambda line: int(line.split()[1]))
                if not sorted_chunk:
                    # end of input
                    break

                #create temporary file to store sorted chunks
                chunk_file = tempfile.NamedTemporaryFile(delete=False)
                chunk_names.append(chunk_file.name)

                #write-in sorted chunk
                with open(chunk_file.name, 'w') as file:
                    file.writelines(sorted_chunk)

        with ExitStack() as stack, open(output, 'w') as output_file:
            files = [stack.enter_context(open(chunk)) for chunk in chunk_names]
            output_file.writelines(merge(*files, key=lambda line: int(line.split()[1])))
        
    except FileNotFoundError:
        print('File {} does not exist'.format(filename))

def concatenate (file1, file2):
    with open('res.txt', 'w') as res, open(file1) as f1, open(file2) as f2:
        for line1, line2 in zip(f1, f2):
            res.write("{} {}\n".format(line1.split()[0], line2.rstrip()))
            '''To do: rubni slucajevi 
            npr. ako u jednom fileu nema ID-ja kojeg ima u drugom
            Possible solution: Dask Dataframe'''

def main():
    #sorting a file with names
    external_sort('names.txt', 'names_sorted.txt')
    #sorting a file with surnames
    external_sort('surnames.txt', 'surnames_sorted.txt')
    concatenate('names_sorted.txt', 'surnames_sorted.txt')



if __name__ == "__main__":
    main()







