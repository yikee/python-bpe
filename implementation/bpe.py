def bpe(sub_data_iter, last_epoch=False, max_merge_time=500, merge_freq_require=10):
    sub_data = [d for d in sub_data_iter]

    word_dict = {}
    whole_word_dict = {}
    for word in sub_data:
        for char in word:
            word_dict[char] = word_dict.get(char, 0) + 1
    
    merge_round = 0
    while True:
        max_freq_wordpiece = max(word_dict, key=word_dict.get)
        max_freq = word_dict[max_freq_wordpiece]

        if max_freq <= merge_freq_require:
            break
        if merge_round > max_merge_time:
            break

        pair_dict = {}
        need_update_word = []
        for idx, char_lst in enumerate(sub_data):
            if max_freq_wordpiece not in char_lst:
                continue
            need_update_word.append(idx)
            for i, wp in enumerate(char_lst):
                if wp != max_freq_wordpiece:
                    continue
                if i < len(char_lst) - 1:
                    pair = (char_lst[i], char_lst[i+1])
                    pair_dict[pair] = pair_dict.get(pair, 0) + 1
                if i > 0:
                    pair = (char_lst[i-1], char_lst[i])
                    pair_dict[pair] = pair_dict.get(pair, 0) + 1

        if not pair_dict:
            whole_word_dict[max_freq_wordpiece] = word_dict.pop(max_freq_wordpiece)
            continue

        merge_round += 1
        to_merge_pair = max(pair_dict, key=pair_dict.get)
        if merge_round % 100 == 1:
            print("merge round: %s, merge pair: %s, frequency:%s" % (merge_round, to_merge_pair, max_freq))
        
        true_freq = 0
        for word_idx in need_update_word:
            word = sub_data[word_idx]
            new_word = []
            idx = 0
            while idx < len(word):
                if idx < len(word)-1 and to_merge_pair == (word[idx], word[idx+1]):
                    new_word.append(''.join(to_merge_pair))
                    true_freq += 1
                    idx += 2
                else:
                    new_word.append(word[idx])
                    idx += 1
                sub_data[word_idx] = new_word

            for wp in to_merge_pair:
                try:
                    if word_dict[wp] == true_freq:
                        word_dict.pop(wp)
                    else:
                        word_dict[wp] -= true_freq
                except Exception as e:
                    print("error: %s" % e)
                    print("wp: %s, to_merge_pair: %s, true_freq: %s" % (wp, to_merge_pair, true_freq))
                    print("word_dict key: %s, value: %s, key: %s, value: %s" % (to_merge_pair[0], word_dict.get(to_merge_pair[0]), to_merge_pair[1], word_dict.get(to_merge_pair[1])))
                    wp_freq = 0
                    for line in sub_data:
                        if wp in line:
                            wp_freq += 1
                    print("wp_freq: %s" % wp_freq)

            word_dict[''.join(to_merge_pair)] = true_freq

        if last_epoch:
            res = merge_two_dicts(whole_word_dict, word_dict)
            yield res
        else:
            for encoded_row in sub_data:
                yield encoded_row
            

def merge_two_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z