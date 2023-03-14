#include<list>
#include<iostream>
int main(){
    std::list<int> list1;
    list1.push_front(2);
    cout << list1.size() <<endl;
}