#include <iostream>
#include <list>

int main() {
    std::list<int> l;
    l.push_front(1); // 在链表头插入元素 1
    l.push_front(2); // 在链表头插入元素 2
    l.push_front(3); // 在链表头插入元素 3

    // 输出链表中的元素
    for (auto it = l.begin(); it != l.end(); ++it) {
        std::cout << *it << " ";
    }
    std::cout << std::endl;

    return 0;
}