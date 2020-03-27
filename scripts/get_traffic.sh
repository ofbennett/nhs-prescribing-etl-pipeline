# Hacky little script to get a rough idea of the site's traffic
# Example usage: $ bash get_traffic.sh
num=$(docker logs viz_app | wc -l)
echo "The total hits since the site was deployed is roughly:"
result=$(( num / 12 ))
echo $result