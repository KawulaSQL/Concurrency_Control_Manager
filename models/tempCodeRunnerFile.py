if __name__ == "__main__":
    print("1. Enter sequence")
    print("2. File Input")
    input_choice = input("Enter your option (1 or 2): ")
    
    if (input_choice == 1):
        input_seq = input("Enter sequence (delimited by ';'): ")
    elif input_choice == "2":
        file_name = input("Enter file name: ")
        file_name = "./test/" + file_name
        try:
            with open(file_name, "r") as file:
                input_seq = file.read().strip()
        except FileNotFoundError:
            print("Error: File not found. Please check the file name and path.")
            exit()
    else:
        print("Invalid option. Exiting.")
        exit()

    try:
        tpl = TwoPhaseLocking(input_seq)
        tpl.run()
        print("\nExecution Result:")
        print(tpl.result_string())
        print("\nTransaction History:")
        print(tpl.history_string())
    except Exception as e:
        print(f"Error: {e}")