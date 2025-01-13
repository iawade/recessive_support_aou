import hail as hl
import sys

# Retrieve input and output paths from command line arguments
input_bed = sys.argv[1]  # PLINK .bed file path
output_mt = sys.argv[2]  # Output file prefix

# Initialize Hail with specific thread and memory settings
hl.init(
    master=f'local[32]',  # Use 32 threads
    spark_conf={
        'spark.driver.memory': '100g'  # Allocate 100 GB of RAM
    }
)

# Import the PLINK files (.bed, .bim, .fam)
# The files are expected to be in the same directory, with extensions .bed, .bim, and .fam
# Example input: "ld_prune/allofus_array_{ancestry}_{prune_method}_wise.bed"
plink_data = hl.import_plink(bed=input_bed, 
                             bim=input_bed.replace(".bed", ".bim"), 
                             fam=input_bed.replace(".bed", ".fam"),
                             reference_genome='GRCh38'
                             )

# Export the Hail dataset to a Hail format (e.g., VDS or Hail HDFS format)
# You can choose to export as .vds or .mt for Hail, but typically .mt (MatrixTable) is used
# Save the Hail MatrixTable to the output path
plink_data.write(output_mt, overwrite=True)

print(f"Conversion complete! Output saved to {output_mt}")

