import os
from datasets import load_from_disk, DatasetDict, concatenate_datasets

def load_and_concatenate_datasets(folder_path):
    """Load all datasets from the specified folder and concatenate them."""
    dataset_paths = [
        os.path.join(folder_path, name)
        for name in os.listdir(folder_path)
        if os.path.isdir(os.path.join(folder_path, name))  # Ensure it's a directory
    ]

    # Load all datasets from the paths
    datasets = [load_from_disk(path) for path in dataset_paths]

    # Concatenate all datasets into one
    if datasets:
        hf_dataset_concat = concatenate_datasets(datasets)
        return hf_dataset_concat
    else:
        print("No datasets found in the specified folder.")
        return None

def save_concatenated_dataset(hf_dataset, output_path):
    """Save the concatenated dataset to disk."""
    hf_dataset.save_to_disk(output_path)
    print(f"Concatenated dataset saved to {output_path}")

if __name__ == "__main__":
    # Define the folder containing your datasets
    folder_path = os.path.expanduser('~/data_who')  # Use expanduser to handle the ~ symbol
    output_path = os.path.join(folder_path, 'hf_raw_dataset_iris')

    # Load and concatenate datasets
    hf_dataset_concat = load_and_concatenate_datasets(folder_path)

    # Save the concatenated dataset if it was successfully created
    if hf_dataset_concat:
        save_concatenated_dataset(hf_dataset_concat, output_path)