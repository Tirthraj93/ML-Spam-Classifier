import re, json


def read_labels(file_path):
    """
    Read label index file to get labels (spam/ham) for each file
    and create a dictionary for each file and their label - 1 for spam and 0 for ham

    :param file_path: index file of labels
    :return: dictionary of file and its label
    """

    out_dict = dict()

    labels_read = 0

    with open(file_path, 'r') as f:
        lines = f.read().splitlines()
        for line in lines:
            # line = spam ../data/inmail.1
            split_array = re.split('\\s+', line.strip())
            # get spam
            label = split_array[0]
            # 1 for spam and 0 for ham
            new_label = '0'
            if label == 'spam':
                new_label = '1'
            # get inmail.1
            file_name = re.split('/', split_array[1])[2]
            out_dict[file_name] = new_label

            print 'read label - ', labels_read
            labels_read += 1

    return out_dict


def read_word_into_list(read_file):
    """
    Reads each line into file in an array where each elements

    :param read_file: file to read
    :return: the list read from file
    """

    data_array = []

    print 'Reading - ', read_file

    with open(read_file, 'r') as in_file:
        lines = in_file.read().splitlines()
        for line in lines:
            data_array.append(line.strip())

    return data_array


def read_word_into_dict(read_file):
    """
    Reads each line containing a word into file in a dictionary of format {word: index}

    :param read_file: file to read
    :return: the dicionary of format {word:index}
    """

    data_dict = dict()

    print 'Reading - ', read_file

    with open(read_file, 'r') as in_file:
        lines = in_file.read().splitlines()
        index = 1
        for line in lines:
            data_dict[line.strip()] = index
            index += 1

    return data_dict


def write_json_file(json_data, json_file):
    """
    Writes dictionary into json file

    :param json_data: dictionary to write
    :param json_file: file to dump json data into
    """

    print 'Writing - ', json_file

    with open(json_file, 'w') as out_file:
        json.dump(json_data, out_file)


def read_json_file(json_file):
    """
    Returns dictionary containing contents of given json file

    :param json_file: json file containing json data
    :return: dictionary containing json data
    """

    print 'Reading - ', json_file

    with open(json_file, 'r') as read_file:
        data = json.load(read_file)

    return data


def list_to_file(write_list, write_file):
    """
    Writes given list to a file as each list element in one line

    :param write_list: list to write
    :param write_file: destination file
    """

    print 'writing - ', write_file

    with open(write_file, 'w') as out_file:
        for element in write_list:
            out_line = "{}\n".format(element)
            out_file.write(out_line)


def append_list_to_file(write_list, write_file):
    """
    Appends given list to a file as each list element in one line

    :param write_list: list to write
    :param write_file: destination file
    """

    print '\tappending - ', len(write_list)

    with open(write_file, 'a+') as out_file:
        for element in write_list:
            out_line = "{}\n".format(element)
            out_file.write(out_line)


def read_predict_result_into_list(read_file):
    """
    Reads results file given by predict -b 1 command into a list

    :param read_file: output file to read
    :return: the list of probability estimates derived by logistic regression
    """

    print 'Reading output file...'

    out_list = []

    with open(read_file, 'r') as f_read:
        lines = f_read.read().splitlines()
        lines_length = len(lines)
        for i in range(1, lines_length):
            line_words = lines[i].strip().split(' ')
            out_list.append(line_words[1])

    return out_list
