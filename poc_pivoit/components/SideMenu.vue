<template>
    <section>
        <ol>
            <li v-for="(opt, idx) in listArray" :key="idx">{{ opt }}</li>
        </ol>
    </section>
</template>

<script>
export default {
    props: {
        listArray: Array
    },
    data: () => {
        return {
            // listArray: ['Welcome Page', 'Create/Select WorkSpace']
        }
    }
}
</script>